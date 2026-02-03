from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from random import Random
from statistics import mean, pstdev
from typing import Any, Dict, Iterable, List, Optional

from .utils import (
    finish_value,
    is_cut,
    parse_iso_datetime,
    parse_yardage,
    safe_div,
)


@dataclass
class CourseSimilarity:
    course: Dict[str, Any]
    score: float


def _group_results_by_player(results: Iterable[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for result in results:
        player = result.get("player") or {}
        player_id = player.get("id")
        if not player_id:
            continue
        tournament = result.get("tournament") or {}
        grouped[player_id].append(
            {
                "player": player,
                "tournament": tournament,
                "start_date": parse_iso_datetime(tournament.get("start_date")),
                "position": result.get("position"),
                "position_numeric": result.get("position_numeric"),
                "par_relative_score": result.get("par_relative_score"),
            }
        )
    return grouped


def _sort_recent(results: List[Dict[str, Any]], last_n: int) -> List[Dict[str, Any]]:
    ordered = sorted(results, key=lambda item: item.get("start_date") or datetime.min)
    return ordered[-last_n:]


def build_player_form(
    results: List[Dict[str, Any]],
    *,
    last_n: int = 10,
    min_events: int = 3,
    cut_penalty: int = 80,
) -> List[Dict[str, Any]]:
    grouped = _group_results_by_player(results)
    output: List[Dict[str, Any]] = []

    for player_id, player_results in grouped.items():
        recent = _sort_recent(player_results, last_n)
        if len(recent) < min_events:
            continue

        finishes = [
            finish_value(r.get("position_numeric"), r.get("position"), cut_penalty)
            for r in recent
        ]
        avg_finish = mean(finishes)
        std_finish = pstdev(finishes) if len(finishes) > 1 else 0.0
        top10_rate = safe_div(sum(1 for f in finishes if f <= 10), len(finishes))
        top20_rate = safe_div(sum(1 for f in finishes if f <= 20), len(finishes))
        cut_rate = safe_div(sum(1 for r in recent if is_cut(r.get("position"))), len(recent))

        form_score = (
            (top10_rate * 0.5) + (top20_rate * 0.3) + ((1 - cut_rate) * 0.2)
        ) - (avg_finish / 100)

        output.append(
            {
                "player_id": player_id,
                "player": recent[-1]["player"],
                "starts": len(recent),
                "avg_finish": round(avg_finish, 2),
                "top10_rate": round(top10_rate, 3),
                "top20_rate": round(top20_rate, 3),
                "cut_rate": round(cut_rate, 3),
                "consistency_index": round(1 / (1 + std_finish), 3),
                "form_score": round(form_score, 4),
                "recent_finishes": [
                    r.get("position") or str(r.get("position_numeric") or "")
                    for r in recent
                ],
            }
        )

    return sorted(output, key=lambda row: row["form_score"], reverse=True)


def build_placement_probabilities(
    results: List[Dict[str, Any]],
    *,
    last_n: int = 20,
    min_events: int = 5,
) -> List[Dict[str, Any]]:
    grouped = _group_results_by_player(results)
    output: List[Dict[str, Any]] = []

    for player_id, player_results in grouped.items():
        recent = _sort_recent(player_results, last_n)
        if len(recent) < min_events:
            continue

        finishes = [
            r.get("position_numeric")
            for r in recent
            if r.get("position_numeric") is not None
        ]
        total = len(recent)
        output.append(
            {
                "player_id": player_id,
                "player": recent[-1]["player"],
                "starts": total,
                "win_prob": round(safe_div(sum(1 for f in finishes if f == 1), total), 3),
                "top5_prob": round(safe_div(sum(1 for f in finishes if f <= 5), total), 3),
                "top10_prob": round(safe_div(sum(1 for f in finishes if f <= 10), total), 3),
                "top20_prob": round(safe_div(sum(1 for f in finishes if f <= 20), total), 3),
            }
        )

    return sorted(output, key=lambda row: row["win_prob"], reverse=True)


def build_cut_rates(
    results: List[Dict[str, Any]],
    *,
    last_n: int = 20,
    min_events: int = 5,
) -> List[Dict[str, Any]]:
    grouped = _group_results_by_player(results)
    output: List[Dict[str, Any]] = []

    for player_id, player_results in grouped.items():
        recent = _sort_recent(player_results, last_n)
        if len(recent) < min_events:
            continue

        cuts = sum(1 for r in recent if is_cut(r.get("position")))
        output.append(
            {
                "player_id": player_id,
                "player": recent[-1]["player"],
                "starts": len(recent),
                "cuts": cuts,
                "cut_rate": round(safe_div(cuts, len(recent)), 3),
                "made_cut_rate": round(1 - safe_div(cuts, len(recent)), 3),
            }
        )

    return sorted(output, key=lambda row: row["cut_rate"], reverse=True)


def build_tournament_difficulty(
    stats: List[Dict[str, Any]],
    *,
    include_rounds: bool = False,
) -> List[Dict[str, Any]]:
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in stats:
        if row.get("round_number") is not None and not include_rounds:
            continue
        tournament = row.get("tournament") or {}
        tournament_id = tournament.get("id")
        if not tournament_id:
            continue
        grouped[tournament_id].append(row)

    output: List[Dict[str, Any]] = []
    for tournament_id, rows in grouped.items():
        scoring_avg = [r.get("scoring_average") for r in rows if r.get("scoring_average") is not None]
        scoring_diff = [r.get("scoring_diff") for r in rows if r.get("scoring_diff") is not None]
        difficulty = [r.get("difficulty_rank") for r in rows if r.get("difficulty_rank") is not None]

        eagles = sum(r.get("eagles") or 0 for r in rows)
        birdies = sum(r.get("birdies") or 0 for r in rows)
        pars = sum(r.get("pars") or 0 for r in rows)
        bogeys = sum(r.get("bogeys") or 0 for r in rows)
        double_bogeys = sum(r.get("double_bogeys") or 0 for r in rows)
        total_scores = eagles + birdies + pars + bogeys + double_bogeys

        output.append(
            {
                "tournament_id": tournament_id,
                "tournament": rows[0].get("tournament"),
                "scoring_average": round(mean(scoring_avg), 3) if scoring_avg else None,
                "scoring_diff": round(mean(scoring_diff), 3) if scoring_diff else None,
                "difficulty_rank": round(mean(difficulty), 2) if difficulty else None,
                "birdie_rate": round(safe_div(eagles + birdies, total_scores), 3),
                "bogey_rate": round(safe_div(bogeys + double_bogeys, total_scores), 3),
                "par_rate": round(safe_div(pars, total_scores), 3),
            }
        )

    return sorted(output, key=lambda row: (row["difficulty_rank"] or 0))


def build_course_profile(holes: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not holes:
        return {"course": None, "summary": {}, "holes": []}

    ordered = sorted(holes, key=lambda h: h.get("hole_number") or 0)
    pars = [h.get("par") for h in ordered if h.get("par") is not None]
    yardages = [h.get("yardage") for h in ordered if h.get("yardage") is not None]

    par3 = sum(1 for p in pars if p == 3)
    par4 = sum(1 for p in pars if p == 4)
    par5 = sum(1 for p in pars if p == 5)

    summary = {
        "par3_count": par3,
        "par4_count": par4,
        "par5_count": par5,
        "total_par": sum(pars) if pars else None,
        "avg_yardage": round(mean(yardages), 1) if yardages else None,
        "total_yardage": sum(yardages) if yardages else None,
        "longest_hole": max(yardages) if yardages else None,
        "shortest_hole": min(yardages) if yardages else None,
    }

    return {
        "course": ordered[0].get("course"),
        "summary": summary,
        "holes": ordered,
    }


def course_similarity(a: Dict[str, Any], b: Dict[str, Any]) -> float:
    score = 0.0
    if a.get("par") is not None and b.get("par") is not None:
        par_diff = abs(a["par"] - b["par"])
        score += 0.4 * (1 - min(par_diff / 5, 1))

    yardage_a = parse_yardage(a.get("yardage"))
    yardage_b = parse_yardage(b.get("yardage"))
    if yardage_a and yardage_b:
        yard_diff = abs(yardage_a - yardage_b)
        score += 0.4 * (1 - min(yard_diff / 2000, 1))

    if a.get("green_grass") and a.get("green_grass") == b.get("green_grass"):
        score += 0.1
    if a.get("fairway_grass") and a.get("fairway_grass") == b.get("fairway_grass"):
        score += 0.1
    return round(score, 4)


def build_course_comps(
    courses: List[Dict[str, Any]],
    target_course_id: int,
    *,
    limit: int = 8,
) -> Dict[str, Any]:
    target = next((c for c in courses if c.get("id") == target_course_id), None)
    if not target:
        return {"course": None, "comps": []}

    comps: List[CourseSimilarity] = []
    for course in courses:
        if course.get("id") == target_course_id:
            continue
        score = course_similarity(target, course)
        comps.append(CourseSimilarity(course=course, score=score))

    comps_sorted = sorted(comps, key=lambda item: item.score, reverse=True)[:limit]
    return {
        "course": target,
        "comps": [
            {
                "course": item.course,
                "similarity": item.score,
            }
            for item in comps_sorted
        ],
    }


def build_course_fit(
    results: List[Dict[str, Any]],
    tournaments: List[Dict[str, Any]],
    courses: List[Dict[str, Any]],
    *,
    target_course_id: int,
    last_n: int = 20,
    min_events: int = 2,
) -> Dict[str, Any]:
    comps_payload = build_course_comps(courses, target_course_id, limit=8)
    comp_course_ids = [c["course"]["id"] for c in comps_payload["comps"]]

    course_tournament_ids: set[int] = set()
    comp_tournament_ids: set[int] = set()

    for tournament in tournaments:
        for entry in tournament.get("courses") or []:
            course = entry.get("course") or {}
            course_id = course.get("id")
            if course_id == target_course_id:
                course_tournament_ids.add(tournament["id"])
            if course_id in comp_course_ids:
                comp_tournament_ids.add(tournament["id"])

    grouped = _group_results_by_player(results)
    players: List[Dict[str, Any]] = []
    for player_id, player_results in grouped.items():
        player_recent = _sort_recent(player_results, last_n)
        course_results = [r for r in player_recent if r["tournament"].get("id") in course_tournament_ids]
        comp_results = [r for r in player_recent if r["tournament"].get("id") in comp_tournament_ids]

        if len(course_results) < min_events and len(comp_results) < min_events:
            continue

        course_finishes = [
            r.get("position_numeric")
            for r in course_results
            if r.get("position_numeric") is not None
        ]
        comp_finishes = [
            r.get("position_numeric")
            for r in comp_results
            if r.get("position_numeric") is not None
        ]

        course_avg = mean(course_finishes) if course_finishes else None
        comp_avg = mean(comp_finishes) if comp_finishes else None

        if course_avg is not None and comp_avg is not None:
            fit_score = (course_avg * 0.7) + (comp_avg * 0.3)
        elif course_avg is not None:
            fit_score = course_avg
        elif comp_avg is not None:
            fit_score = comp_avg
        else:
            continue

        players.append(
            {
                "player_id": player_id,
                "player": player_results[-1]["player"],
                "course_events": len(course_results),
                "comp_events": len(comp_results),
                "course_avg_finish": round(course_avg, 2) if course_avg is not None else None,
                "comp_avg_finish": round(comp_avg, 2) if comp_avg is not None else None,
                "course_fit_score": round(fit_score, 2),
            }
        )

    players_sorted = sorted(players, key=lambda item: item["course_fit_score"])
    return {
        "course": comps_payload["course"],
        "comps": comps_payload["comps"],
        "players": players_sorted,
    }


def build_matchup(
    results: List[Dict[str, Any]],
    *,
    player_id: int,
    opponent_id: int,
    cut_penalty: int = 80,
) -> Dict[str, Any]:
    player_map: Dict[int, Dict[str, Any]] = {}
    opponent_map: Dict[int, Dict[str, Any]] = {}

    for result in results:
        tournament = result.get("tournament") or {}
        tournament_id = tournament.get("id")
        if not tournament_id:
            continue
        pid = (result.get("player") or {}).get("id")
        if pid == player_id:
            player_map[tournament_id] = result
        if pid == opponent_id:
            opponent_map[tournament_id] = result

    common_ids = sorted(set(player_map.keys()) & set(opponent_map.keys()))
    matches: List[Dict[str, Any]] = []
    wins = losses = ties = 0

    for tournament_id in common_ids:
        player_result = player_map[tournament_id]
        opponent_result = opponent_map[tournament_id]

        player_finish = finish_value(
            player_result.get("position_numeric"),
            player_result.get("position"),
            cut_penalty,
        )
        opponent_finish = finish_value(
            opponent_result.get("position_numeric"),
            opponent_result.get("position"),
            cut_penalty,
        )

        if player_finish < opponent_finish:
            wins += 1
        elif player_finish > opponent_finish:
            losses += 1
        else:
            ties += 1

        matches.append(
            {
                "tournament": player_result.get("tournament"),
                "player_position": player_result.get("position"),
                "opponent_position": opponent_result.get("position"),
                "player_finish_value": player_finish,
                "opponent_finish_value": opponent_finish,
            }
        )

    total = wins + losses + ties
    return {
        "player_id": player_id,
        "opponent_id": opponent_id,
        "matches": matches,
        "starts": total,
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "win_rate": round(safe_div(wins, total), 3),
    }


def build_region_splits(
    results: List[Dict[str, Any]],
    *,
    player_id: int,
) -> Dict[str, Any]:
    player_results = [r for r in results if (r.get("player") or {}).get("id") == player_id]

    by_month: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    by_country: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for result in player_results:
        tournament = result.get("tournament") or {}
        start_date = parse_iso_datetime(tournament.get("start_date"))
        if start_date:
            by_month[start_date.month].append(result)

        country = tournament.get("country") or "Unknown"
        by_country[country].append(result)

    def summarize(group: Dict[Any, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        rows = []
        for key, items in group.items():
            finishes = [r.get("position_numeric") for r in items if r.get("position_numeric") is not None]
            top10 = sum(1 for f in finishes if f <= 10)
            rows.append(
                {
                    "key": key,
                    "starts": len(items),
                    "avg_finish": round(mean(finishes), 2) if finishes else None,
                    "top10_rate": round(safe_div(top10, len(items)), 3),
                }
            )
        return rows

    return {
        "player_id": player_id,
        "by_month": sorted(summarize(by_month), key=lambda r: r["key"]),
        "by_country": sorted(summarize(by_country), key=lambda r: r["starts"], reverse=True),
    }


def build_simulated_finishes(
    results: List[Dict[str, Any]],
    *,
    player_id: int,
    last_n: int = 20,
    simulations: int = 2000,
    cut_penalty: int = 80,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    player_results = [
        {
            "position_numeric": r.get("position_numeric"),
            "position": r.get("position"),
            "start_date": parse_iso_datetime((r.get("tournament") or {}).get("start_date")),
        }
        for r in results
        if (r.get("player") or {}).get("id") == player_id
    ]
    recent = _sort_recent(player_results, last_n)
    if not recent:
        return {"player_id": player_id, "simulations": 0, "distribution": []}

    finish_values = [
        finish_value(r.get("position_numeric"), r.get("position"), cut_penalty)
        for r in recent
    ]
    rng = Random(seed or (player_id + last_n))

    bins = {
        "1-5": 0,
        "6-10": 0,
        "11-20": 0,
        "21-30": 0,
        "31-50": 0,
        "51+": 0,
        "cut": 0,
    }

    for _ in range(simulations):
        finish = rng.choice(finish_values)
        if finish >= cut_penalty:
            bins["cut"] += 1
        elif finish <= 5:
            bins["1-5"] += 1
        elif finish <= 10:
            bins["6-10"] += 1
        elif finish <= 20:
            bins["11-20"] += 1
        elif finish <= 30:
            bins["21-30"] += 1
        elif finish <= 50:
            bins["31-50"] += 1
        else:
            bins["51+"] += 1

    def rate(count: int) -> float:
        return round(safe_div(count, simulations), 3)

    return {
        "player_id": player_id,
        "simulations": simulations,
        "distribution": {key: rate(value) for key, value in bins.items()},
        "top5_prob": rate(bins["1-5"]),
        "top10_prob": rate(bins["1-5"] + bins["6-10"]),
        "top20_prob": rate(bins["1-5"] + bins["6-10"] + bins["11-20"]),
    }
from __future__ import annotations

import math
import random
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .client import fetch_paginated
from .utils import (
    finish_value,
    is_cut,
    parse_iso_datetime,
    parse_yardage,
    safe_div,
)


def get_players(
    *,
    search: Optional[str] = None,
    active: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if search:
        params["search"] = search
    if active is not None:
        params["active"] = str(active).lower()
    return fetch_paginated("/players", params=params, cache_ttl=3600)


def get_tournaments(
    *,
    season: Optional[int] = None,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if season:
        params["season"] = season
    if status:
        params["status"] = status
    return fetch_paginated("/tournaments", params=params, cache_ttl=3600)


def get_courses(
    *,
    search: Optional[str] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if search:
        params["search"] = search
    return fetch_paginated("/courses", params=params, cache_ttl=3600)


def get_tournament_results(
    *,
    season: Optional[int] = None,
    tournament_ids: Optional[List[int]] = None,
    player_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if season:
        params["season"] = season
    if tournament_ids:
        params["tournament_ids"] = tournament_ids
    if player_ids:
        params["player_ids"] = player_ids
    return fetch_paginated("/tournament_results", params=params, cache_ttl=900)


def get_tournament_course_stats(
    *,
    tournament_ids: Optional[List[int]] = None,
    course_ids: Optional[List[int]] = None,
    hole_number: Optional[int] = None,
    round_number: Optional[int] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if tournament_ids:
        params["tournament_ids"] = tournament_ids
    if course_ids:
        params["course_ids"] = course_ids
    if hole_number:
        params["hole_number"] = hole_number
    if round_number:
        params["round_number"] = round_number
    return fetch_paginated("/tournament_course_stats", params=params, cache_ttl=900)


def get_course_holes(
    *,
    course_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if course_ids:
        params["course_ids"] = course_ids
    return fetch_paginated("/course_holes", params=params, cache_ttl=3600)


def _group_results_by_player_v2(results: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    grouped: Dict[int, Dict[str, Any]] = {}
    for row in results:
        player = row.get("player") or {}
        player_id = player.get("id")
        if not player_id:
            continue
        tournament = row.get("tournament") or {}
        start_date = parse_iso_datetime(tournament.get("start_date"))
        grouped.setdefault(
            player_id,
            {"player": player, "results": []},
        )["results"].append(
            {
                "tournament": tournament,
                "start_date": start_date,
                "position": row.get("position"),
                "position_numeric": row.get("position_numeric"),
                "par_relative_score": row.get("par_relative_score"),
            }
        )
    return grouped


def _recent_results(entries: List[Dict[str, Any]], last_n: int) -> List[Dict[str, Any]]:
    ordered = sorted(
        entries,
        key=lambda item: item.get("start_date") or datetime.min,
    )
    return ordered[-last_n:]


def _calc_std(values: List[int]) -> float:
    if not values:
        return 0.0
    mean_val = sum(values) / len(values)
    variance = sum((val - mean_val) ** 2 for val in values) / len(values)
    return math.sqrt(variance)


def player_form_summary(
    results: List[Dict[str, Any]],
    *,
    last_n: int,
    min_events: int,
    cut_penalty: int = 80,
) -> List[Dict[str, Any]]:
    grouped = _group_results_by_player_v2(results)
    summaries: List[Dict[str, Any]] = []
    for player_id, payload in grouped.items():
        recent = _recent_results(payload["results"], last_n)
        if len(recent) < min_events:
            continue
        finish_values = [
            finish_value(r.get("position_numeric"), r.get("position"), cut_penalty)
            for r in recent
        ]
        avg_finish = sum(finish_values) / len(finish_values)
        top10 = sum(1 for r in recent if (r.get("position_numeric") or 999) <= 10)
        top20 = sum(1 for r in recent if (r.get("position_numeric") or 999) <= 20)
        cuts = sum(1 for r in recent if is_cut(r.get("position")))
        std_finish = _calc_std(finish_values)
        consistency = 1 / (1 + std_finish)
        top10_rate = safe_div(top10, len(recent))
        top20_rate = safe_div(top20, len(recent))
        cut_rate = safe_div(cuts, len(recent))
        form_score = (
            (1 - (avg_finish / 100)) * 0.45
            + top10_rate * 0.35
            + top20_rate * 0.2
        ) * (1 - cut_rate * 0.25)

        summaries.append(
            {
                "player": payload["player"],
                "starts": len(recent),
                "avg_finish": round(avg_finish, 2),
                "top10_rate": round(top10_rate, 3),
                "top20_rate": round(top20_rate, 3),
                "cut_rate": round(cut_rate, 3),
                "consistency_index": round(consistency, 4),
                "form_score": round(form_score, 4),
                "recent_finishes": [
                    r.get("position") or r.get("position_numeric") or "NA"
                    for r in recent
                ],
            }
        )
    return sorted(summaries, key=lambda item: item["form_score"], reverse=True)


def placement_probabilities(
    results: List[Dict[str, Any]],
    *,
    last_n: int,
    min_events: int,
) -> List[Dict[str, Any]]:
    grouped = _group_results_by_player_v2(results)
    summaries: List[Dict[str, Any]] = []
    for payload in grouped.values():
        recent = _recent_results(payload["results"], last_n)
        if len(recent) < min_events:
            continue
        win = sum(1 for r in recent if (r.get("position_numeric") or 999) == 1)
        top5 = sum(1 for r in recent if (r.get("position_numeric") or 999) <= 5)
        top10 = sum(1 for r in recent if (r.get("position_numeric") or 999) <= 10)
        top20 = sum(1 for r in recent if (r.get("position_numeric") or 999) <= 20)
        summaries.append(
            {
                "player": payload["player"],
                "starts": len(recent),
                "win_prob": round(safe_div(win, len(recent)), 3),
                "top5_prob": round(safe_div(top5, len(recent)), 3),
                "top10_prob": round(safe_div(top10, len(recent)), 3),
                "top20_prob": round(safe_div(top20, len(recent)), 3),
            }
        )
    return sorted(summaries, key=lambda item: item["win_prob"], reverse=True)


def cut_rates(
    results: List[Dict[str, Any]],
    *,
    last_n: int,
    min_events: int,
) -> List[Dict[str, Any]]:
    grouped = _group_results_by_player_v2(results)
    summaries: List[Dict[str, Any]] = []
    for payload in grouped.values():
        recent = _recent_results(payload["results"], last_n)
        if len(recent) < min_events:
            continue
        cuts = sum(1 for r in recent if is_cut(r.get("position")))
        summaries.append(
            {
                "player": payload["player"],
                "starts": len(recent),
                "cuts": cuts,
                "cut_rate": round(safe_div(cuts, len(recent)), 3),
                "made_cut_rate": round(1 - safe_div(cuts, len(recent)), 3),
            }
        )
    return sorted(summaries, key=lambda item: item["cut_rate"], reverse=True)


def matchup_summary(
    results: List[Dict[str, Any]],
    *,
    player_id: int,
    opponent_id: int,
    cut_penalty: int = 80,
) -> Dict[str, Any]:
    player_rows = [r for r in results if (r.get("player") or {}).get("id") == player_id]
    opp_rows = [r for r in results if (r.get("player") or {}).get("id") == opponent_id]

    player_by_tournament = {r["tournament"]["id"]: r for r in player_rows if r.get("tournament")}
    opp_by_tournament = {r["tournament"]["id"]: r for r in opp_rows if r.get("tournament")}

    wins = losses = ties = 0
    meetings: List[Dict[str, Any]] = []
    for tournament_id, row in player_by_tournament.items():
        opp_row = opp_by_tournament.get(tournament_id)
        if not opp_row:
            continue
        player_finish = finish_value(
            row.get("position_numeric"), row.get("position"), cut_penalty
        )
        opp_finish = finish_value(
            opp_row.get("position_numeric"), opp_row.get("position"), cut_penalty
        )
        if player_finish < opp_finish:
            wins += 1
        elif player_finish > opp_finish:
            losses += 1
        else:
            ties += 1
        meetings.append(
            {
                "tournament": row.get("tournament"),
                "player_position": row.get("position"),
                "opponent_position": opp_row.get("position"),
            }
        )

    total = wins + losses + ties
    return {
        "player_id": player_id,
        "opponent_id": opponent_id,
        "meetings": total,
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "win_rate": round(safe_div(wins, total), 3),
        "matchups": meetings,
    }


def _course_similarity(course: Dict[str, Any], other: Dict[str, Any]) -> float:
    if course["id"] == other["id"]:
        return -1.0
    par_score = 1.0 if course.get("par") == other.get("par") else 0.7
    yardage_a = parse_yardage(course.get("yardage"))
    yardage_b = parse_yardage(other.get("yardage"))
    if yardage_a and yardage_b:
        diff = abs(yardage_a - yardage_b)
        yardage_score = max(0.0, 1 - diff / 1500)
    else:
        yardage_score = 0.5
    green_score = 1.0 if course.get("green_grass") == other.get("green_grass") else 0.5
    fairway_score = (
        1.0 if course.get("fairway_grass") == other.get("fairway_grass") else 0.5
    )
    return par_score * 0.35 + yardage_score * 0.35 + green_score * 0.15 + fairway_score * 0.15


def course_comps(
    courses: List[Dict[str, Any]],
    *,
    course_id: int,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    course = next((c for c in courses if c.get("id") == course_id), None)
    if not course:
        return []
    scored = []
    for other in courses:
        score = _course_similarity(course, other)
        if score <= 0:
            continue
        scored.append(
            {
                "course": other,
                "similarity": round(score, 4),
            }
        )
    scored.sort(key=lambda item: item["similarity"], reverse=True)
    return scored[:limit]


def course_fit(
    results_by_season: Dict[int, List[Dict[str, Any]]],
    tournaments_by_season: Dict[int, List[Dict[str, Any]]],
    *,
    course_id: int,
    min_events: int,
    cut_penalty: int = 80,
) -> Dict[str, Any]:
    courses = get_courses()
    comps = course_comps(courses, course_id=course_id, limit=5)
    comp_course_ids = {item["course"]["id"] for item in comps}
    target_course = next((c for c in courses if c.get("id") == course_id), None)

    tournament_ids = set()
    comp_tournament_ids = set()
    for season, tournaments in tournaments_by_season.items():
        for tournament in tournaments:
            course_entries = tournament.get("courses") or []
            used_course_ids = {
                (entry.get("course") or {}).get("id") for entry in course_entries
            }
            if course_id in used_course_ids:
                tournament_ids.add(tournament.get("id"))
            if used_course_ids & comp_course_ids:
                comp_tournament_ids.add(tournament.get("id"))

    player_bucket: Dict[int, Dict[str, Any]] = defaultdict(
        lambda: {
            "player": None,
            "course_finishes": [],
            "comp_finishes": [],
        }
    )

    for season, results in results_by_season.items():
        for row in results:
            tournament = row.get("tournament") or {}
            tournament_id = tournament.get("id")
            player = row.get("player") or {}
            player_id = player.get("id")
            if not player_id:
                continue
            if tournament_id in tournament_ids:
                player_bucket[player_id]["player"] = player
                player_bucket[player_id]["course_finishes"].append(
                    finish_value(
                        row.get("position_numeric"),
                        row.get("position"),
                        cut_penalty,
                    )
                )
            if tournament_id in comp_tournament_ids:
                player_bucket[player_id]["player"] = player
                player_bucket[player_id]["comp_finishes"].append(
                    finish_value(
                        row.get("position_numeric"),
                        row.get("position"),
                        cut_penalty,
                    )
                )

    rows: List[Dict[str, Any]] = []
    for payload in player_bucket.values():
        course_finishes = payload["course_finishes"]
        comp_finishes = payload["comp_finishes"]
        if len(course_finishes) < min_events and len(comp_finishes) < min_events:
            continue
        course_avg = (
            sum(course_finishes) / len(course_finishes)
            if course_finishes
            else None
        )
        comp_avg = sum(comp_finishes) / len(comp_finishes) if comp_finishes else None
        weighted_avg = None
        if course_avg is not None and comp_avg is not None:
            weighted_avg = (course_avg * 0.6) + (comp_avg * 0.4)
        elif course_avg is not None:
            weighted_avg = course_avg
        elif comp_avg is not None:
            weighted_avg = comp_avg
        rows.append(
            {
                "player": payload["player"],
                "course_events": len(course_finishes),
                "comp_events": len(comp_finishes),
                "course_avg_finish": round(course_avg, 2) if course_avg is not None else None,
                "comp_avg_finish": round(comp_avg, 2) if comp_avg is not None else None,
                "course_fit_score": round(weighted_avg, 2) if weighted_avg is not None else None,
            }
        )

    rows.sort(
        key=lambda item: item["course_fit_score"]
        if item["course_fit_score"] is not None
        else 999
    )

    return {
        "course": target_course,
        "comps": comps,
        "rows": rows,
    }


def tournament_difficulty_summary(
    stats: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    grouped: Dict[int, Dict[str, Any]] = defaultdict(
        lambda: {
            "tournament": None,
            "hole_stats": [],
            "scoring_avg": [],
            "scoring_diff": [],
            "difficulty": [],
            "eagles": 0,
            "birdies": 0,
            "pars": 0,
            "bogeys": 0,
            "double_bogeys": 0,
        }
    )
    for row in stats:
        tournament = row.get("tournament") or {}
        tournament_id = tournament.get("id")
        if not tournament_id:
            continue
        payload = grouped[tournament_id]
        payload["tournament"] = tournament
        if row.get("scoring_average") is not None:
            payload["scoring_avg"].append(row.get("scoring_average"))
        if row.get("scoring_diff") is not None:
            payload["scoring_diff"].append(row.get("scoring_diff"))
        if row.get("difficulty_rank") is not None:
            payload["difficulty"].append(row.get("difficulty_rank"))
        payload["eagles"] += row.get("eagles") or 0
        payload["birdies"] += row.get("birdies") or 0
        payload["pars"] += row.get("pars") or 0
        payload["bogeys"] += row.get("bogeys") or 0
        payload["double_bogeys"] += row.get("double_bogeys") or 0

    summaries: List[Dict[str, Any]] = []
    for payload in grouped.values():
        total_scores = (
            payload["eagles"]
            + payload["birdies"]
            + payload["pars"]
            + payload["bogeys"]
            + payload["double_bogeys"]
        )
        summaries.append(
            {
                "tournament": payload["tournament"],
                "scoring_average": round(
                    sum(payload["scoring_avg"]) / len(payload["scoring_avg"])
                    if payload["scoring_avg"]
                    else 0,
                    3,
                ),
                "scoring_diff": round(
                    sum(payload["scoring_diff"]) / len(payload["scoring_diff"])
                    if payload["scoring_diff"]
                    else 0,
                    3,
                ),
                "avg_difficulty_rank": round(
                    sum(payload["difficulty"]) / len(payload["difficulty"])
                    if payload["difficulty"]
                    else 0,
                    2,
                ),
                "birdie_rate": round(
                    safe_div(payload["birdies"], total_scores),
                    3,
                ),
                "bogey_rate": round(
                    safe_div(payload["bogeys"] + payload["double_bogeys"], total_scores),
                    3,
                ),
            }
        )
    return summaries


def course_profile(
    holes: List[Dict[str, Any]],
) -> Dict[str, Any]:
    par3 = [h for h in holes if h.get("par") == 3]
    par4 = [h for h in holes if h.get("par") == 4]
    par5 = [h for h in holes if h.get("par") == 5]
    yardages = [h.get("yardage") for h in holes if h.get("yardage")]
    avg_yardage = sum(yardages) / len(yardages) if yardages else 0
    total_yardage = sum(yardages) if yardages else 0
    longest = sorted(holes, key=lambda h: h.get("yardage") or 0, reverse=True)[:3]
    return {
        "summary": {
            "holes": len(holes),
            "par3_count": len(par3),
            "par4_count": len(par4),
            "par5_count": len(par5),
            "avg_yardage": round(avg_yardage, 1),
            "total_yardage": total_yardage,
        },
        "longest_holes": longest,
        "holes": holes,
    }


def region_splits(
    results: List[Dict[str, Any]],
    *,
    player_id: int,
) -> Dict[str, Any]:
    month_bucket: Dict[str, List[int]] = defaultdict(list)
    country_bucket: Dict[str, List[int]] = defaultdict(list)
    for row in results:
        player = row.get("player") or {}
        if player.get("id") != player_id:
            continue
        tournament = row.get("tournament") or {}
        start_date = parse_iso_datetime(tournament.get("start_date"))
        month_key = start_date.strftime("%b") if start_date else "Unknown"
        country_key = tournament.get("country") or "Unknown"
        finish = row.get("position_numeric")
        if finish is None:
            continue
        month_bucket[month_key].append(finish)
        country_bucket[country_key].append(finish)

    def summarize(bucket: Dict[str, List[int]]) -> List[Dict[str, Any]]:
        rows = []
        for key, finishes in bucket.items():
            top10 = sum(1 for f in finishes if f <= 10)
            rows.append(
                {
                    "label": key,
                    "starts": len(finishes),
                    "avg_finish": round(sum(finishes) / len(finishes), 2),
                    "top10_rate": round(safe_div(top10, len(finishes)), 3),
                }
            )
        return sorted(rows, key=lambda item: item["avg_finish"])

    return {
        "player_id": player_id,
        "by_month": summarize(month_bucket),
        "by_country": summarize(country_bucket),
    }


def simulate_finishes(
    results: List[Dict[str, Any]],
    *,
    player_id: int,
    last_n: int,
    simulations: int,
    cut_penalty: int = 80,
) -> Dict[str, Any]:
    player_rows = [r for r in results if (r.get("player") or {}).get("id") == player_id]
    grouped = _group_results_by_player_v2(player_rows)
    if player_id not in grouped:
        return {"player_id": player_id, "simulations": 0, "distribution": []}
    recent = _recent_results(grouped[player_id]["results"], last_n)
    sample = [
        finish_value(r.get("position_numeric"), r.get("position"), cut_penalty)
        for r in recent
    ]
    if not sample:
        return {"player_id": player_id, "simulations": 0, "distribution": []}

    rng = random.Random(f"{player_id}:{last_n}:{len(sample)}")
    outcomes = [rng.choice(sample) for _ in range(simulations)]

    bins = {
        "1-5": sum(1 for o in outcomes if o <= 5),
        "6-10": sum(1 for o in outcomes if 6 <= o <= 10),
        "11-20": sum(1 for o in outcomes if 11 <= o <= 20),
        "21-30": sum(1 for o in outcomes if 21 <= o <= 30),
        "31+": sum(1 for o in outcomes if o > 30),
    }
    distribution = [
        {"bucket": key, "count": count, "prob": round(safe_div(count, simulations), 3)}
        for key, count in bins.items()
    ]

    return {
        "player_id": player_id,
        "simulations": simulations,
        "top5_prob": round(safe_div(sum(1 for o in outcomes if o <= 5), simulations), 3),
        "top10_prob": round(safe_div(sum(1 for o in outcomes if o <= 10), simulations), 3),
        "top20_prob": round(safe_div(sum(1 for o in outcomes if o <= 20), simulations), 3),
        "distribution": distribution,
    }


def _normalize_metric(
    values: Dict[int, Optional[float]],
    *,
    invert: bool = False,
    default: float = 0.5,
) -> Dict[int, float]:
    valid = [value for value in values.values() if value is not None]
    if not valid:
        return {key: default for key in values}
    min_val = min(valid)
    max_val = max(valid)
    if min_val == max_val:
        return {key: default for key in values}

    normalized: Dict[int, float] = {}
    for key, value in values.items():
        if value is None:
            normalized[key] = default
            continue
        score = (value - min_val) / (max_val - min_val)
        normalized[key] = 1 - score if invert else score
    return normalized


def _tournament_bonus(
    results: List[Dict[str, Any]],
    *,
    player_id: int,
    tournament_id: int,
    cut_penalty: int,
) -> Dict[str, Any]:
    finishes: List[int] = []
    for row in results:
        tournament = row.get("tournament") or {}
        if tournament.get("id") != tournament_id:
            continue
        player = row.get("player") or {}
        if player.get("id") != player_id:
            continue
        finishes.append(
            finish_value(row.get("position_numeric"), row.get("position"), cut_penalty)
        )
    if not finishes:
        return {"bonus": 0.0, "avg_finish": None, "starts": 0}

    avg_finish = sum(finishes) / len(finishes)
    raw_bonus = (40 - avg_finish) / 40
    bonus = max(0.0, min(1.0, raw_bonus))
    return {
        "bonus": round(bonus, 3),
        "avg_finish": round(avg_finish, 2),
        "starts": len(finishes),
    }


def _edge_label(edge: float) -> str:
    if edge >= 0.08:
        return "Best Edge"
    if edge >= 0.04:
        return "Lean"
    return "No Clear Edge"


def build_compare(
    results: List[Dict[str, Any]],
    *,
    player_ids: List[int],
    players: Optional[List[Dict[str, Any]]] = None,
    tournaments: Optional[List[Dict[str, Any]]] = None,
    courses: Optional[List[Dict[str, Any]]] = None,
    course_id: Optional[int] = None,
    tournament_id: Optional[int] = None,
    last_n_form: int = 10,
    last_n_placement: int = 20,
    cut_penalty: int = 80,
) -> Dict[str, Any]:
    player_ids = list(dict.fromkeys(player_ids))
    players_map: Dict[int, Dict[str, Any]] = {
        (player.get("id")): player for player in (players or []) if player.get("id")
    }
    for row in results:
        player = row.get("player") or {}
        player_id = player.get("id")
        if player_id and player_id not in players_map:
            players_map[player_id] = player

    form_rows = build_player_form(results, last_n=last_n_form, min_events=1)
    placement_rows = build_placement_probabilities(
        results, last_n=last_n_placement, min_events=1
    )
    form_map = {row["player_id"]: row for row in form_rows}
    placement_map = {row["player_id"]: row for row in placement_rows}

    course_fit_map: Dict[int, Dict[str, Any]] = {}
    if course_id and tournaments and courses:
        course_payload = build_course_fit(
            results,
            tournaments,
            courses,
            target_course_id=course_id,
            last_n=last_n_placement,
            min_events=1,
        )
        for row in course_payload.get("players", []):
            course_fit_map[row["player_id"]] = row

    head_to_head: List[Dict[str, Any]] = []
    h2h_rates: Dict[int, List[float]] = {pid: [] for pid in player_ids}
    h2h_starts: Dict[int, int] = {pid: 0 for pid in player_ids}

    for idx, player_id in enumerate(player_ids):
        for opponent_id in player_ids[idx + 1 :]:
            matchup = build_matchup(
                results,
                player_id=player_id,
                opponent_id=opponent_id,
                cut_penalty=cut_penalty,
            )
            starts = matchup.get("starts") or 0
            wins = matchup.get("wins") or 0
            losses = matchup.get("losses") or 0
            ties = matchup.get("ties") or 0

            head_to_head.append(
                {
                    "player_id": player_id,
                    "opponent_id": opponent_id,
                    "starts": starts,
                    "wins": wins,
                    "losses": losses,
                    "ties": ties,
                    "win_rate": matchup.get("win_rate"),
                }
            )

            if starts > 0:
                h2h_rates[player_id].append(matchup.get("win_rate") or 0.0)
                h2h_rates[opponent_id].append(safe_div(losses, starts))
                h2h_starts[player_id] += starts
                h2h_starts[opponent_id] += starts

    metrics: Dict[int, Dict[str, Any]] = {}
    for player_id in player_ids:
        form = form_map.get(player_id, {})
        placement = placement_map.get(player_id, {})
        course_fit = course_fit_map.get(player_id, {})
        h2h_rate = (
            sum(h2h_rates[player_id]) / len(h2h_rates[player_id])
            if h2h_rates[player_id]
            else None
        )
        tournament_history = (
            _tournament_bonus(
                results,
                player_id=player_id,
                tournament_id=tournament_id,
                cut_penalty=cut_penalty,
            )
            if tournament_id
            else {"bonus": 0.0, "avg_finish": None, "starts": 0}
        )

        metrics[player_id] = {
            "form_score": form.get("form_score"),
            "course_fit_score": course_fit.get("course_fit_score"),
            "head_to_head_win_rate": round(h2h_rate, 3) if h2h_rate is not None else None,
            "head_to_head_starts": h2h_starts[player_id],
            "top5_prob": placement.get("top5_prob"),
            "top10_prob": placement.get("top10_prob"),
            "top20_prob": placement.get("top20_prob"),
            "tournament_bonus": tournament_history["bonus"],
            "tournament_avg_finish": tournament_history["avg_finish"],
            "tournament_starts": tournament_history["starts"],
        }

    form_norm = _normalize_metric(
        {pid: metrics[pid]["form_score"] for pid in player_ids}
    )
    course_norm = _normalize_metric(
        {pid: metrics[pid]["course_fit_score"] for pid in player_ids},
        invert=True,
    )
    h2h_norm = _normalize_metric(
        {pid: metrics[pid]["head_to_head_win_rate"] for pid in player_ids}
    )
    top10_norm = _normalize_metric(
        {pid: metrics[pid]["top10_prob"] for pid in player_ids}
    )

    base_weights = {
        "form": 0.4,
        "course_fit": 0.25,
        "head_to_head": 0.2,
        "top10": 0.15,
    }
    if course_id is None:
        base_weights.pop("course_fit")
    total_weight = sum(base_weights.values())
    weights = {key: value / total_weight for key, value in base_weights.items()}

    player_rows: List[Dict[str, Any]] = []
    for player_id in player_ids:
        score = (
            weights.get("form", 0) * form_norm[player_id]
            + weights.get("course_fit", 0) * course_norm[player_id]
            + weights.get("head_to_head", 0) * h2h_norm[player_id]
            + weights.get("top10", 0) * top10_norm[player_id]
        )
        if tournament_id:
            score += 0.05 * (metrics[player_id]["tournament_bonus"] or 0)

        player_rows.append(
            {
                "player_id": player_id,
                "player": players_map.get(player_id, {"id": player_id}),
                "score": round(score, 4),
                "metrics": metrics[player_id],
            }
        )

    player_rows.sort(key=lambda row: row["score"], reverse=True)
    for index, row in enumerate(player_rows, start=1):
        row["rank"] = index

    recommendation = None
    if player_rows:
        best = player_rows[0]
        second_score = player_rows[1]["score"] if len(player_rows) > 1 else 0.0
        edge = round(best["score"] - second_score, 4)
        label = _edge_label(edge)

        reasons: List[str] = []
        metric_candidates = [
            ("Best recent form", form_norm),
            ("Strongest course fit", course_norm),
            ("Head-to-head edge", h2h_norm),
            ("Top-10 probability leader", top10_norm),
        ]
        for text, metric in metric_candidates:
            best_pid = max(metric, key=metric.get)
            if best_pid != best["player_id"]:
                continue
            values = sorted(metric.values(), reverse=True)
            margin = values[0] - values[1] if len(values) > 1 else values[0]
            if margin >= 0.02:
                reasons.append(text)

        if tournament_id and (best["metrics"].get("tournament_bonus") or 0) >= 0.4:
            reasons.append("Strong tournament history")

        if not reasons:
            reasons = ["Composite score leader"]

        recommendation = {
            "player_id": best["player_id"],
            "label": label,
            "edge": edge,
            "reasons": reasons[:2],
        }

    return {
        "player_ids": player_ids,
        "course_id": course_id,
        "tournament_id": tournament_id,
        "weights": weights,
        "players": player_rows,
        "head_to_head": head_to_head,
        "recommendation": recommendation,
    }
