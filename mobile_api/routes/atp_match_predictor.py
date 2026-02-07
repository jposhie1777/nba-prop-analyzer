"""
ATP Match Predictor – synthesises form, H2H, surface splits, and ranking
into a single match prediction with win probability, set score distribution,
and tiebreak likelihood.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from atp.analytics import (
    build_compare,
    build_head_to_head,
    build_player_form,
    build_set_distribution,
    build_surface_splits,
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
) -> List[int]:
    if season is not None:
        return [season]
    current = _current_season()
    back = seasons_back if seasons_back is not None else 2
    return [current - offset for offset in range(back + 1)]


def _fetch_matches(seasons: List[int], max_pages: Optional[int] = 5) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for s in seasons:
        matches.extend(
            fetch_paginated("/matches", params={"season": s}, cache_ttl=900, max_pages=max_pages)
        )
    return matches


def _predict_set_score(
    p1_straight_sets_rate: float,
    p1_win_prob: float,
    is_best_of_5: bool,
) -> Dict[str, Any]:
    """Estimate set score distribution from win probability and dominance rate."""
    p2_win_prob = 1 - p1_win_prob
    p2_straight_rate = max(0.3, 1 - p1_straight_sets_rate * 0.8)

    if is_best_of_5:
        scores = {
            "3-0": round(p1_win_prob * p1_straight_sets_rate * 0.5, 3),
            "3-1": round(p1_win_prob * (1 - p1_straight_sets_rate) * 0.6, 3),
            "3-2": round(p1_win_prob * (1 - p1_straight_sets_rate) * 0.4, 3),
            "0-3": round(p2_win_prob * p2_straight_rate * 0.5, 3),
            "1-3": round(p2_win_prob * (1 - p2_straight_rate) * 0.6, 3),
            "2-3": round(p2_win_prob * (1 - p2_straight_rate) * 0.4, 3),
        }
    else:
        scores = {
            "2-0": round(p1_win_prob * p1_straight_sets_rate, 3),
            "2-1": round(p1_win_prob * (1 - p1_straight_sets_rate), 3),
            "0-2": round(p2_win_prob * p2_straight_rate, 3),
            "1-2": round(p2_win_prob * (1 - p2_straight_rate), 3),
        }

    # Normalize
    total = sum(scores.values())
    if total > 0:
        scores = {k: round(v / total, 3) for k, v in scores.items()}

    return scores


def _confidence_label(prob: float) -> str:
    if prob >= 0.70:
        return "High"
    if prob >= 0.55:
        return "Moderate"
    return "Low"


@router.get("/analytics/match-predictor")
def atp_match_predictor(
    player_id: int = Query(..., description="Player 1 ID"),
    opponent_id: int = Query(..., description="Player 2 ID"),
    surface: Optional[str] = Query(None, description="Surface filter (Hard, Clay, Grass)"),
    season: Optional[int] = None,
    seasons_back: Optional[int] = Query(2, ge=0, le=10),
    last_n: int = Query(12, ge=3, le=60),
    best_of_5: bool = Query(False, description="Grand Slam (best of 5) format"),
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    """
    Predict the outcome of a head-to-head matchup between two ATP players.

    Combines form, surface splits, head-to-head history, set distribution
    patterns, and ranking into a unified win probability.
    """
    try:
        if player_id == opponent_id:
            raise HTTPException(status_code=400, detail="player_id and opponent_id must differ")

        seasons = _resolve_seasons(season=season, seasons_back=seasons_back)
        matches = _fetch_matches(seasons, max_pages=max_pages)

        # Fetch rankings
        rankings_payload = fetch_paginated(
            "/rankings", params={"per_page": 100}, cache_ttl=900, max_pages=3
        )
        rankings_map: Dict[int, int] = {}
        for row in rankings_payload:
            player = row.get("player") or {}
            pid = player.get("id")
            if pid:
                rankings_map[pid] = row.get("rank")

        # 1) Compare (composite scores)
        compare = build_compare(
            matches,
            player_ids=[player_id, opponent_id],
            surface=surface,
            last_n=last_n,
            rankings=rankings_map,
        )

        # 2) Head-to-head detail
        h2h = build_head_to_head(matches, player_id=player_id, opponent_id=opponent_id)

        # 3) Surface splits for each
        p1_surfaces = build_surface_splits(matches, player_id=player_id, min_matches=3)
        p2_surfaces = build_surface_splits(matches, player_id=opponent_id, min_matches=3)

        # 4) Set distribution for each
        p1_sets = build_set_distribution(matches, player_id=player_id, surface=surface)
        p2_sets = build_set_distribution(matches, player_id=opponent_id, surface=surface)

        # 5) Form for each player
        form_rows = build_player_form(matches, last_n=last_n, min_matches=3, surface=surface)
        form_map = {r["player_id"]: r for r in form_rows}

        # Extract scores from compare
        player_scores = {p["player_id"]: p for p in compare.get("players", [])}
        p1_data = player_scores.get(player_id, {})
        p2_data = player_scores.get(opponent_id, {})

        p1_score = p1_data.get("score", 0.5)
        p2_score = p2_data.get("score", 0.5)

        # Win probability from normalized composite scores
        total_score = p1_score + p2_score
        if total_score > 0:
            p1_win_prob = round(p1_score / total_score, 3)
        else:
            p1_win_prob = 0.5

        p1_form = form_map.get(player_id, {})
        p2_form = form_map.get(opponent_id, {})

        p1_straight_rate = p1_form.get("straight_sets_rate", 0.4)
        p2_straight_rate = p2_form.get("straight_sets_rate", 0.4)

        # Set score predictions
        set_scores = _predict_set_score(p1_straight_rate, p1_win_prob, best_of_5)

        # Tiebreak probability from both players' rates
        p1_tb_rate = p1_form.get("tiebreak_rate", 0.2)
        p2_tb_rate = p2_form.get("tiebreak_rate", 0.2)
        tiebreak_prob = round((p1_tb_rate + p2_tb_rate) / 2, 3)

        # Surface-specific data
        surface_lower = (surface or "").lower()
        p1_surface_row = next(
            (s for s in p1_surfaces if s["surface"].lower() == surface_lower), None
        ) if surface else None
        p2_surface_row = next(
            (s for s in p2_surfaces if s["surface"].lower() == surface_lower), None
        ) if surface else None

        # Fetch player names
        player_names: Dict[int, str] = {}
        for pid in [player_id, opponent_id]:
            try:
                payload = fetch_one_page("/players", params={"player_ids": [pid]}, cache_ttl=300)
                for p in payload.get("data", []):
                    name = p.get("full_name") or f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
                    player_names[p["id"]] = name
            except Exception:
                pass

        # Build insights
        insights: List[str] = []
        if p1_win_prob >= 0.60:
            insights.append(f"{player_names.get(player_id, 'Player 1')} is the clear favorite")
        elif p1_win_prob <= 0.40:
            insights.append(f"{player_names.get(opponent_id, 'Player 2')} is the clear favorite")
        else:
            insights.append("This is a closely matched contest")

        if h2h.get("starts", 0) >= 3:
            h2h_wr = h2h.get("win_rate", 0.5)
            if h2h_wr >= 0.65:
                insights.append(
                    f"{player_names.get(player_id, 'Player 1')} dominates the head-to-head ({h2h['wins']}-{h2h['losses']})"
                )
            elif h2h_wr <= 0.35:
                insights.append(
                    f"{player_names.get(opponent_id, 'Player 2')} dominates the head-to-head"
                )

        if surface and p1_surface_row and p2_surface_row:
            if p1_surface_row["win_rate"] > p2_surface_row["win_rate"] + 0.1:
                insights.append(
                    f"{player_names.get(player_id, 'Player 1')} has a {surface} advantage "
                    f"({p1_surface_row['win_rate']:.0%} vs {p2_surface_row['win_rate']:.0%})"
                )
            elif p2_surface_row["win_rate"] > p1_surface_row["win_rate"] + 0.1:
                insights.append(
                    f"{player_names.get(opponent_id, 'Player 2')} has a {surface} advantage "
                    f"({p2_surface_row['win_rate']:.0%} vs {p1_surface_row['win_rate']:.0%})"
                )

        if tiebreak_prob >= 0.30:
            insights.append("High tiebreak probability — expect tight sets")

        # Build response
        return {
            "player_id": player_id,
            "opponent_id": opponent_id,
            "player_name": player_names.get(player_id),
            "opponent_name": player_names.get(opponent_id),
            "surface": surface,
            "best_of_5": best_of_5,
            "seasons": seasons,
            "prediction": {
                "win_probability": p1_win_prob,
                "loss_probability": round(1 - p1_win_prob, 3),
                "confidence": _confidence_label(max(p1_win_prob, 1 - p1_win_prob)),
                "predicted_winner_id": player_id if p1_win_prob >= 0.5 else opponent_id,
                "predicted_winner_name": (
                    player_names.get(player_id) if p1_win_prob >= 0.5
                    else player_names.get(opponent_id)
                ),
                "set_scores": set_scores,
                "tiebreak_probability": tiebreak_prob,
            },
            "factors": {
                "player": {
                    "player_id": player_id,
                    "name": player_names.get(player_id),
                    "composite_score": p1_score,
                    "ranking": rankings_map.get(player_id),
                    "form_score": p1_form.get("form_score"),
                    "win_rate": p1_form.get("win_rate"),
                    "straight_sets_rate": p1_form.get("straight_sets_rate"),
                    "tiebreak_rate": p1_form.get("tiebreak_rate"),
                    "recent_results": (p1_form.get("recent_results") or [])[:5],
                    "surface_record": (
                        {
                            "matches": p1_surface_row["matches"],
                            "win_rate": p1_surface_row["win_rate"],
                        }
                        if p1_surface_row
                        else None
                    ),
                },
                "opponent": {
                    "player_id": opponent_id,
                    "name": player_names.get(opponent_id),
                    "composite_score": p2_score,
                    "ranking": rankings_map.get(opponent_id),
                    "form_score": p2_form.get("form_score"),
                    "win_rate": p2_form.get("win_rate"),
                    "straight_sets_rate": p2_form.get("straight_sets_rate"),
                    "tiebreak_rate": p2_form.get("tiebreak_rate"),
                    "recent_results": (p2_form.get("recent_results") or [])[:5],
                    "surface_record": (
                        {
                            "matches": p2_surface_row["matches"],
                            "win_rate": p2_surface_row["win_rate"],
                        }
                        if p2_surface_row
                        else None
                    ),
                },
                "head_to_head": {
                    "starts": h2h.get("starts", 0),
                    "wins": h2h.get("wins", 0),
                    "losses": h2h.get("losses", 0),
                    "win_rate": h2h.get("win_rate", 0),
                    "by_surface": h2h.get("by_surface", []),
                    "recent_matches": (h2h.get("matches") or [])[-5:],
                },
                "weights": compare.get("weights", {}),
            },
            "insights": insights,
        }
    except HTTPException:
        raise
    except Exception as err:
        _handle_error(err)
