from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROUND_ORDER = {
    "finals": 7,
    "final": 7,
    "semifinals": 6,
    "semi finals": 6,
    "semifinal": 6,
    "semi final": 6,
    "semis": 6,
    "quarterfinals": 5,
    "quarter finals": 5,
    "quarterfinal": 5,
    "quarter final": 5,
    "quarters": 5,
    "round of 16": 4,
    "fourth round": 4,
    "round of 32": 3,
    "third round": 3,
    "round of 64": 2,
    "second round": 2,
    "round of 128": 1,
    "first round": 1,
    "qualifying": 0,
    "qualification": 0,
    "3rd round qualifying": -1,
    "2nd round qualifying": -2,
    "1st round qualifying": -3,
    "q3": -1,
    "q2": -2,
    "q1": -3,
}

_SCORE_RE = re.compile(r"(\d+)-(\d+)")
_RETIRE_TOKENS = ("W/O", "WO", "RET", "DEF", "ABD", "ABN")


def _round_rank(round_name: Optional[str]) -> int:
    if not round_name:
        return 0
    key = round_name.lower().replace("-", " ").strip()
    return ROUND_ORDER.get(key, 0)


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_score(score: Optional[str]) -> Optional[Dict[str, int]]:
    if not score:
        return None
    upper = score.upper()
    if any(token in upper for token in _RETIRE_TOKENS):
        return None

    sets: List[Tuple[int, int]] = []
    for part in score.strip().split():
        match = _SCORE_RE.search(part)
        if not match:
            continue
        a = int(match.group(1))
        b = int(match.group(2))
        sets.append((a, b))

    if not sets:
        return None

    winner_sets = sum(1 for a, b in sets if a > b)
    loser_sets = sum(1 for a, b in sets if a < b)
    tiebreak_sets = 0
    for a, b in sets:
        hi, lo = max(a, b), min(a, b)
        if (hi == 7 and lo >= 6 and (hi - lo) == 1) or (hi >= 10 and (hi - lo) == 2):
            tiebreak_sets += 1

    return {
        "winner_sets": winner_sets,
        "loser_sets": loser_sets,
        "total_sets": winner_sets + loser_sets,
        "tiebreak_sets": tiebreak_sets,
    }


def _safe_div(numer: float, denom: float) -> float:
    if denom == 0:
        return 0.0
    return numer / denom


def _calc_std(values: List[float]) -> float:
    if not values:
        return 0.0
    mean_val = sum(values) / len(values)
    variance = sum((v - mean_val) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def _sort_entries(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        entries,
        key=lambda item: (item.get("start_date") or datetime.min, item.get("match_id") or 0),
    )


def build_player_entries(matches: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    bucket: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for match in matches:
        player1 = match.get("player1") or {}
        player2 = match.get("player2") or {}
        winner = match.get("winner") or {}

        player1_id = player1.get("id")
        player2_id = player2.get("id")
        winner_id = winner.get("id")
        if not player1_id or not player2_id:
            continue

        tournament = match.get("tournament") or {}
        start_date = _parse_date(tournament.get("start_date")) or _parse_date(
            tournament.get("end_date")
        )
        score_info = _parse_score(match.get("score"))

        for player, opponent in ((player1, player2), (player2, player1)):
            player_id = player.get("id")
            opponent_id = opponent.get("id")
            if not player_id or not opponent_id:
                continue

            is_winner = player_id == winner_id
            sets_won = None
            sets_lost = None
            total_sets = None
            tiebreak = False
            straight_sets_win = False

            if score_info:
                if is_winner:
                    sets_won = score_info["winner_sets"]
                    sets_lost = score_info["loser_sets"]
                else:
                    sets_won = score_info["loser_sets"]
                    sets_lost = score_info["winner_sets"]
                total_sets = score_info["total_sets"]
                tiebreak = score_info["tiebreak_sets"] > 0
                straight_sets_win = bool(is_winner and sets_lost == 0 and total_sets)

            bucket[player_id].append(
                {
                    "match_id": match.get("id"),
                    "player_id": player_id,
                    "player": player,
                    "opponent_id": opponent_id,
                    "opponent": opponent,
                    "result": "W" if is_winner else "L",
                    "score": match.get("score"),
                    "duration": match.get("duration"),
                    "number_of_sets": match.get("number_of_sets"),
                    "surface": tournament.get("surface"),
                    "category": tournament.get("category"),
                    "season": match.get("season") or tournament.get("season"),
                    "round": match.get("round"),
                    "tournament": tournament,
                    "tournament_id": tournament.get("id"),
                    "tournament_name": tournament.get("name"),
                    "start_date": start_date,
                    "sets_won": sets_won,
                    "sets_lost": sets_lost,
                    "total_sets": total_sets,
                    "tiebreak": tiebreak,
                    "straight_sets_win": straight_sets_win,
                }
            )

    return bucket


def _recent(entries: List[Dict[str, Any]], last_n: int) -> List[Dict[str, Any]]:
    ordered = _sort_entries(entries)
    return ordered[-last_n:]


def build_player_form(
    matches: List[Dict[str, Any]],
    *,
    last_n: int = 10,
    min_matches: int = 5,
    surface: Optional[str] = None,
) -> List[Dict[str, Any]]:
    entries_by_player = build_player_entries(matches)
    rows: List[Dict[str, Any]] = []

    for player_id, entries in entries_by_player.items():
        if surface:
            filtered = [e for e in entries if (e.get("surface") or "").lower() == surface.lower()]
        else:
            filtered = entries

        if len(filtered) < min_matches:
            continue

        recent = _recent(filtered, last_n)
        wins = sum(1 for e in recent if e["result"] == "W")
        matches_count = len(recent)
        win_rate = _safe_div(wins, matches_count)

        straight_sets_wins = sum(1 for e in recent if e.get("straight_sets_win"))
        straight_sets_rate = _safe_div(straight_sets_wins, max(wins, 1))

        total_sets_values = [e.get("total_sets") for e in recent if e.get("total_sets")]
        avg_sets = (
            round(sum(total_sets_values) / len(total_sets_values), 2)
            if total_sets_values
            else None
        )

        tiebreak_rate = _safe_div(sum(1 for e in recent if e.get("tiebreak")), matches_count)

        form_score = round(
            (win_rate * 0.65) + (straight_sets_rate * 0.2) + ((1 - tiebreak_rate) * 0.15),
            4,
        )

        recent_results = []
        for entry in recent:
            opponent = entry.get("opponent") or {}
            opponent_name = opponent.get("last_name") or opponent.get("full_name") or "Opponent"
            recent_results.append(f"{entry['result']} vs {opponent_name}")

        player = recent[-1].get("player") if recent else (entries[0].get("player") if entries else {})

        rows.append(
            {
                "player_id": player_id,
                "player": player,
                "matches": matches_count,
                "wins": wins,
                "win_rate": round(win_rate, 3),
                "straight_sets_rate": round(straight_sets_rate, 3),
                "avg_sets": avg_sets,
                "tiebreak_rate": round(tiebreak_rate, 3),
                "form_score": form_score,
                "recent_results": recent_results,
            }
        )

    return sorted(rows, key=lambda row: row["form_score"], reverse=True)


def build_surface_splits(
    matches: List[Dict[str, Any]],
    *,
    player_id: int,
    min_matches: int = 5,
) -> List[Dict[str, Any]]:
    entries_by_player = build_player_entries(matches)
    entries = entries_by_player.get(player_id, [])
    if not entries:
        return []

    surfaces: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        surface = entry.get("surface") or "Unknown"
        surfaces[surface].append(entry)

    rows: List[Dict[str, Any]] = []
    for surface, group in surfaces.items():
        if len(group) < min_matches:
            continue
        wins = sum(1 for e in group if e["result"] == "W")
        straight_sets_wins = sum(1 for e in group if e.get("straight_sets_win"))
        total_sets = [e.get("total_sets") for e in group if e.get("total_sets")]
        avg_sets = round(sum(total_sets) / len(total_sets), 2) if total_sets else None
        tiebreak_rate = _safe_div(sum(1 for e in group if e.get("tiebreak")), len(group))
        rows.append(
            {
                "surface": surface,
                "matches": len(group),
                "wins": wins,
                "losses": len(group) - wins,
                "win_rate": round(_safe_div(wins, len(group)), 3),
                "straight_sets_rate": round(_safe_div(straight_sets_wins, max(wins, 1)), 3),
                "avg_sets": avg_sets,
                "tiebreak_rate": round(tiebreak_rate, 3),
            }
        )

    return sorted(rows, key=lambda row: row["win_rate"], reverse=True)


def build_head_to_head(
    matches: List[Dict[str, Any]],
    *,
    player_id: int,
    opponent_id: int,
) -> Dict[str, Any]:
    entries_by_player = build_player_entries(matches)
    entries = [
        e
        for e in entries_by_player.get(player_id, [])
        if e.get("opponent_id") == opponent_id
    ]
    if not entries:
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

    ordered = _sort_entries(entries)
    wins = sum(1 for e in ordered if e["result"] == "W")
    by_surface: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entry in ordered:
        by_surface[entry.get("surface") or "Unknown"].append(entry)

    surface_rows = []
    for surface, group in by_surface.items():
        surface_wins = sum(1 for e in group if e["result"] == "W")
        surface_rows.append(
            {
                "surface": surface,
                "matches": len(group),
                "wins": surface_wins,
                "losses": len(group) - surface_wins,
                "win_rate": round(_safe_div(surface_wins, len(group)), 3),
            }
        )

    matches_payload = [
        {
            "tournament": entry.get("tournament"),
            "round": entry.get("round"),
            "surface": entry.get("surface"),
            "result": entry.get("result"),
            "score": entry.get("score"),
            "start_date": entry.get("tournament", {}).get("start_date"),
        }
        for entry in ordered
    ]

    return {
        "player_id": player_id,
        "opponent_id": opponent_id,
        "starts": len(ordered),
        "wins": wins,
        "losses": len(ordered) - wins,
        "win_rate": round(_safe_div(wins, len(ordered)), 3),
        "by_surface": sorted(surface_rows, key=lambda row: row["win_rate"], reverse=True),
        "matches": matches_payload,
    }


def build_tournament_performance(
    matches: List[Dict[str, Any]],
    *,
    min_matches: int = 5,
    surface: Optional[str] = None,
) -> List[Dict[str, Any]]:
    entries_by_player = build_player_entries(matches)
    rows: List[Dict[str, Any]] = []

    for player_id, entries in entries_by_player.items():
        if surface:
            entries = [e for e in entries if (e.get("surface") or "").lower() == surface.lower()]
        if len(entries) < min_matches:
            continue

        wins = sum(1 for e in entries if e["result"] == "W")
        tournaments: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            tournament_id = entry.get("tournament_id")
            if tournament_id:
                tournaments[tournament_id].append(entry)

        titles = finals = semis = quarters = 0
        for tournament_entries in tournaments.values():
            best_round = max((_round_rank(e.get("round")) for e in tournament_entries), default=0)
            is_final = best_round >= _round_rank("Finals")
            is_semi = best_round >= _round_rank("Semifinals")
            is_quarter = best_round >= _round_rank("Quarterfinals")

            finals += 1 if is_final else 0
            semis += 1 if is_semi else 0
            quarters += 1 if is_quarter else 0

            won_final = any(
                _round_rank(e.get("round")) >= _round_rank("Finals") and e["result"] == "W"
                for e in tournament_entries
            )
            titles += 1 if won_final else 0

        player = entries[-1].get("player") if entries else {}
        rows.append(
            {
                "player_id": player_id,
                "player": player,
                "tournaments": len(tournaments),
                "titles": titles,
                "finals": finals,
                "semis": semis,
                "quarters": quarters,
                "match_wins": wins,
                "match_losses": len(entries) - wins,
                "win_rate": round(_safe_div(wins, len(entries)), 3),
            }
        )

    return sorted(
        rows,
        key=lambda row: (row["titles"], row["finals"], row["win_rate"]),
        reverse=True,
    )


def build_region_splits(
    matches: List[Dict[str, Any]],
    *,
    player_id: int,
) -> Dict[str, Any]:
    entries_by_player = build_player_entries(matches)
    entries = entries_by_player.get(player_id, [])
    if not entries:
        return {"player_id": player_id, "by_month": [], "by_location": []}

    by_month: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    by_location: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        tournament = entry.get("tournament") or {}
        start_date = _parse_date(tournament.get("start_date"))
        if start_date:
            by_month[start_date.month].append(entry)
        location = tournament.get("location") or "Unknown"
        by_location[location].append(entry)

    def _summarize(bucket: Dict[Any, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        rows = []
        for key, items in bucket.items():
            wins = sum(1 for e in items if e["result"] == "W")
            rows.append(
                {
                    "key": key,
                    "matches": len(items),
                    "wins": wins,
                    "win_rate": round(_safe_div(wins, len(items)), 3),
                }
            )
        return rows

    return {
        "player_id": player_id,
        "by_month": sorted(_summarize(by_month), key=lambda row: row["key"]),
        "by_location": sorted(_summarize(by_location), key=lambda row: row["matches"], reverse=True),
    }


def build_set_distribution(
    matches: List[Dict[str, Any]],
    *,
    player_id: int,
    surface: Optional[str] = None,
) -> Dict[str, Any]:
    entries_by_player = build_player_entries(matches)
    entries = entries_by_player.get(player_id, [])
    if surface:
        entries = [e for e in entries if (e.get("surface") or "").lower() == surface.lower()]

    wins_dist: Dict[str, int] = defaultdict(int)
    losses_dist: Dict[str, int] = defaultdict(int)

    for entry in entries:
        sets_won = entry.get("sets_won")
        sets_lost = entry.get("sets_lost")
        if sets_won is None or sets_lost is None:
            continue
        key = f"{sets_won}-{sets_lost}"
        if entry["result"] == "W":
            wins_dist[key] += 1
        else:
            losses_dist[key] += 1

    wins_total = sum(wins_dist.values())
    losses_total = sum(losses_dist.values())

    wins_rates = {
        key: round(_safe_div(count, wins_total), 3) for key, count in wins_dist.items()
    }
    losses_rates = {
        key: round(_safe_div(count, losses_total), 3) for key, count in losses_dist.items()
    }

    return {
        "player_id": player_id,
        "surface": surface,
        "wins": wins_dist,
        "losses": losses_dist,
        "win_rates": wins_rates,
        "loss_rates": losses_rates,
    }


def _normalize_metric(
    values: Dict[int, Optional[float]],
    *,
    invert: bool = False,
    default: float = 0.5,
) -> Dict[int, float]:
    valid = [v for v in values.values() if v is not None]
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


def build_compare(
    matches: List[Dict[str, Any]],
    *,
    player_ids: List[int],
    surface: Optional[str] = None,
    last_n: int = 12,
    rankings: Optional[Dict[int, int]] = None,
) -> Dict[str, Any]:
    player_ids = list(dict.fromkeys(player_ids))
    entries_by_player = build_player_entries(matches)

    metrics: Dict[int, Dict[str, Any]] = {}
    for player_id in player_ids:
        entries = entries_by_player.get(player_id, [])
        if surface:
            entries_surface = [
                e for e in entries if (e.get("surface") or "").lower() == surface.lower()
            ]
        else:
            entries_surface = entries

        recent = _recent(entries_surface, last_n)
        wins = sum(1 for e in recent if e["result"] == "W")
        win_rate = _safe_div(wins, len(recent)) if recent else 0.0

        straight_sets_wins = sum(1 for e in recent if e.get("straight_sets_win"))
        straight_sets_rate = _safe_div(straight_sets_wins, max(wins, 1)) if recent else 0.0

        tiebreak_rate = _safe_div(sum(1 for e in recent if e.get("tiebreak")), len(recent)) if recent else 0.0

        form_score = (win_rate * 0.65) + (straight_sets_rate * 0.2) + ((1 - tiebreak_rate) * 0.15)

        overall_wins = sum(1 for e in entries_surface if e["result"] == "W")
        overall_rate = _safe_div(overall_wins, len(entries_surface)) if entries_surface else None

        metrics[player_id] = {
            "form_score": round(form_score, 4),
            "recent_win_rate": round(win_rate, 3),
            "surface_win_rate": round(overall_rate, 3) if overall_rate is not None else None,
            "ranking": rankings.get(player_id) if rankings else None,
        }

    head_to_head: Dict[str, Any] = {}
    if len(player_ids) >= 2:
        head_to_head = build_head_to_head(
            matches,
            player_id=player_ids[0],
            opponent_id=player_ids[1],
        )

    if head_to_head and head_to_head.get("starts"):
        h2h_rate = head_to_head.get("win_rate")
        h2h_map = {
            player_ids[0]: h2h_rate,
            player_ids[1]: 1 - h2h_rate if h2h_rate is not None else None,
        }
    else:
        h2h_map = {pid: None for pid in player_ids}

    form_norm = _normalize_metric({pid: metrics[pid]["form_score"] for pid in player_ids})
    surface_norm = _normalize_metric({pid: metrics[pid]["surface_win_rate"] for pid in player_ids})
    h2h_norm = _normalize_metric(h2h_map)
    rank_norm = _normalize_metric(
        {pid: metrics[pid]["ranking"] for pid in player_ids},
        invert=True,
    )

    weights = {
        "form": 0.45,
        "surface": 0.25,
        "head_to_head": 0.2,
        "ranking": 0.1,
    }

    players_payload = []
    for pid in player_ids:
        score = (
            weights["form"] * form_norm[pid]
            + weights["surface"] * surface_norm[pid]
            + weights["head_to_head"] * h2h_norm[pid]
            + weights["ranking"] * rank_norm[pid]
        )
        players_payload.append(
            {
                "player_id": pid,
                "score": round(score, 4),
                "metrics": metrics[pid],
            }
        )

    players_payload.sort(key=lambda row: row["score"], reverse=True)
    for index, row in enumerate(players_payload, start=1):
        row["rank"] = index

    recommendation = None
    if players_payload:
        best = players_payload[0]
        runner_up = players_payload[1]["score"] if len(players_payload) > 1 else 0.0
        edge = round(best["score"] - runner_up, 4)
        if edge >= 0.08:
            label = "Best Edge"
        elif edge >= 0.04:
            label = "Lean"
        else:
            label = "No Clear Edge"
        recommendation = {
            "player_id": best["player_id"],
            "label": label,
            "edge": edge,
            "reasons": ["Composite leader"],
        }

    return {
        "player_ids": player_ids,
        "surface": surface,
        "weights": weights,
        "players": players_payload,
        "head_to_head": head_to_head,
        "recommendation": recommendation,
    }
