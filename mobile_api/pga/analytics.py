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
