"""
ATP Match Projection Engine.

Joins FanDuel sportsbook odds with player analytics to produce projections
and edges for moneyline, total games, game spread, set spread, and player
total markets.

Data sources:
  - sportsbook.raw_fanduel_atp_markets  (FanDuel odds, scraped daily)
  - atp_data.atp_betting_analytics       (form scores, surface rates, streaks)
  - atp_data.sackmann_player_surface_features (avg games/sets per match by surface)
  - atp_data.sackmann_h2h_features       (head-to-head avg games/sets)
"""

from __future__ import annotations

import logging
import math
import re
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery

from bq import get_bq_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FANDUEL_ATP_TABLE = "sportsbook.raw_fanduel_atp_markets"
BETTING_ANALYTICS_TABLE = "atp_data.atp_betting_analytics"
SACKMANN_SURFACE_TABLE = "atp_data.sackmann_player_surface_features"
SACKMANN_H2H_TABLE = "atp_data.sackmann_h2h_features"

# Tournament name → surface mapping (FanDuel names)
# Updated as new tournaments appear in the data
TOURNAMENT_SURFACE: Dict[str, str] = {
    "atp houston": "clay",
    "atp marrakech": "clay",
    "atp bucharest": "clay",
    "atp monte carlo": "clay",
    "atp barcelona": "clay",
    "atp madrid": "clay",
    "atp rome": "clay",
    "atp roland garros": "clay",
    "atp french open": "clay",
    "atp wimbledon": "grass",
    "atp queen": "grass",
    "atp halle": "grass",
    "atp s-hertogenbosch": "grass",
    "atp stuttgart": "grass",
    "atp eastbourne": "grass",
    "atp us open": "hard",
    "atp australian open": "hard",
    "atp indian wells": "hard",
    "atp miami": "hard",
    "atp cincinnati": "hard",
    "atp montreal": "hard",
    "atp toronto": "hard",
    "atp shanghai": "hard",
    "atp beijing": "hard",
    "atp tokyo": "hard",
    "atp vienna": "hard",
    "atp basel": "hard",
    "atp paris": "hard",
    "atp atp finals": "hard",
}


def _infer_surface(tournament_name: str) -> str:
    """Infer surface from tournament name, defaulting to 'hard'."""
    name_lower = tournament_name.lower().strip()
    for key, surface in TOURNAMENT_SURFACE.items():
        if key in name_lower:
            return surface
    # Clay season heuristic: March–June tournaments in certain regions
    for clay_hint in ("marrakech", "bucharest", "houston", "estoril", "lyon",
                      "geneva", "umag", "bastad", "hamburg", "gstaad",
                      "kitzbuhel", "buenos aires", "rio", "cordoba", "santiago"):
        if clay_hint in name_lower:
            return "clay"
    return "hard"


def _implied_prob(american_odds: int) -> float:
    """Convert American odds to implied probability (no-vig)."""
    if american_odds < 0:
        return -american_odds / (-american_odds + 100)
    return 100 / (american_odds + 100)


def _american_from_prob(prob: float) -> int:
    """Convert probability to American odds."""
    if prob <= 0 or prob >= 1:
        return 0
    if prob >= 0.5:
        return int(round(-100 * prob / (1 - prob)))
    return int(round(100 * (1 - prob) / prob))


def _safe_query(sql: str, params=None) -> List[Dict[str, Any]]:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(query_parameters=list(params or []))
    try:
        job = client.query(sql, job_config=job_config)
        return [dict(row) for row in job.result()]
    except Exception as exc:
        logger.warning("Projection query failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def _fetch_latest_fanduel_odds() -> List[Dict[str, Any]]:
    """Fetch the most recent scrape of FanDuel ATP markets."""
    sql = f"""
    WITH latest_scrape AS (
      SELECT MAX(scraped_at) AS max_ts
      FROM `{FANDUEL_ATP_TABLE}`
      WHERE scraped_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    )
    SELECT
      m.event_id,
      m.event_name,
      m.tournament_name,
      m.player_home,
      m.player_away,
      m.event_start,
      m.market_name,
      m.market_type,
      m.market_id,
      m.selection_id,
      m.selection_name,
      m.handicap,
      m.odds_decimal,
      m.odds_american,
      m.odds_american_prev,
      m.deep_link,
      m.is_inplay,
      m.sgm_market,
      m.runner_status
    FROM `{FANDUEL_ATP_TABLE}` m
    JOIN latest_scrape ls ON m.scraped_at = ls.max_ts
    WHERE m.runner_status = 'ACTIVE'
      AND m.selection_name NOT LIKE '%/%'
    ORDER BY m.event_start, m.event_id, m.market_name
    """
    return _safe_query(sql)


def _fetch_analytics_for_players(player_names: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch betting analytics keyed by (player_name, surface_key)."""
    if not player_names:
        return {}

    sql = f"""
    SELECT
      player_name,
      surface_key,
      world_rank,
      adj_win_rate,
      l10_adj_win_rate,
      l20_adj_win_rate,
      l10_surface_adj_win_rate,
      l20_surface_adj_win_rate,
      win_rate_vs_top50,
      straight_sets_rate,
      tiebreak_rate,
      avg_sets_per_match,
      betting_form_score,
      sample_confidence,
      current_win_streak,
      current_loss_streak
    FROM `{BETTING_ANALYTICS_TABLE}`
    WHERE LOWER(player_name) IN UNNEST(@names)
    """
    rows = _safe_query(sql, [
        bigquery.ArrayQueryParameter("names", "STRING", [n.lower() for n in player_names]),
    ])

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = f"{row['player_name'].lower()}|{row['surface_key']}"
        out[key] = row
    return out


def _fetch_sackmann_features(player_names: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch Sackmann surface features keyed by (player_name_norm, surface_key)."""
    if not player_names:
        return {}

    sql = f"""
    SELECT
      player_name,
      surface_key,
      avg_games_per_match,
      avg_sets_per_match,
      recent_avg_games_l5,
      recent_avg_sets_l5,
      aces_per_match,
      double_faults_per_match
    FROM `{SACKMANN_SURFACE_TABLE}`
    WHERE LOWER(player_name) IN UNNEST(@names)
    """
    rows = _safe_query(sql, [
        bigquery.ArrayQueryParameter("names", "STRING", [n.lower() for n in player_names]),
    ])

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = f"{row['player_name'].lower()}|{row['surface_key']}"
        out[key] = row
    return out


def _fetch_h2h_features(player_names: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch H2H features keyed by 'p1|p2|surface'."""
    if not player_names:
        return {}

    sql = f"""
    SELECT
      player_name,
      opponent_name,
      surface_key,
      matches_played,
      wins,
      losses,
      win_rate,
      avg_games_per_match,
      avg_sets_per_match
    FROM `{SACKMANN_H2H_TABLE}`
    WHERE LOWER(player_name) IN UNNEST(@names)
      AND LOWER(opponent_name) IN UNNEST(@names)
    """
    rows = _safe_query(sql, [
        bigquery.ArrayQueryParameter("names", "STRING", [n.lower() for n in player_names]),
    ])

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = f"{row['player_name'].lower()}|{row['opponent_name'].lower()}|{row['surface_key']}"
        out[key] = row
    return out


# ---------------------------------------------------------------------------
# Projection logic
# ---------------------------------------------------------------------------

def _get_analytics(
    analytics: Dict[str, Dict[str, Any]],
    player_name: str,
    surface: str,
) -> Optional[Dict[str, Any]]:
    """Look up analytics for a player, falling back from surface → all."""
    key = f"{player_name.lower()}|{surface}"
    row = analytics.get(key)
    if row:
        return row
    return analytics.get(f"{player_name.lower()}|all")


def _get_sackmann(
    features: Dict[str, Dict[str, Any]],
    player_name: str,
    surface: str,
) -> Optional[Dict[str, Any]]:
    """Look up sackmann features, falling back from surface → all."""
    key = f"{player_name.lower()}|{surface}"
    row = features.get(key)
    if row:
        return row
    # Try 'all' surface
    for k, v in features.items():
        if k.startswith(f"{player_name.lower()}|"):
            return v
    return None


def _project_moneyline(
    home_analytics: Dict[str, Any],
    away_analytics: Dict[str, Any],
    surface: str,
    h2h: Dict[str, Dict[str, Any]],
    home_name: str,
    away_name: str,
) -> Tuple[float, float]:
    """
    Project moneyline win probability for home/away.

    Weights:
      - Form score (35%): betting_form_score
      - Surface win rate (25%): l10_surface_adj_win_rate or l20_surface_adj_win_rate
      - Recent form (20%): l10_adj_win_rate
      - Ranking (10%): world_rank (inverted)
      - H2H (10%): historical win rate in matchup
    """
    def _form(a: Dict) -> float:
        return a.get("betting_form_score") or a.get("adj_win_rate") or 0.5

    def _surface_rate(a: Dict) -> float:
        return (
            a.get("l10_surface_adj_win_rate")
            or a.get("l20_surface_adj_win_rate")
            or a.get("adj_win_rate")
            or 0.5
        )

    def _recent(a: Dict) -> float:
        return a.get("l10_adj_win_rate") or a.get("adj_win_rate") or 0.5

    def _rank_score(a: Dict) -> float:
        rank = a.get("world_rank")
        if not rank or rank <= 0:
            return 0.5
        # Map rank 1→1.0, rank 100→0.5, rank 200+→0.3
        return max(0.3, 1.0 - (rank - 1) * 0.005)

    h_form, a_form = _form(home_analytics), _form(away_analytics)
    h_surf, a_surf = _surface_rate(home_analytics), _surface_rate(away_analytics)
    h_recent, a_recent = _recent(home_analytics), _recent(away_analytics)
    h_rank, a_rank = _rank_score(home_analytics), _rank_score(away_analytics)

    # H2H
    h2h_key = f"{home_name.lower()}|{away_name.lower()}|{surface}"
    h2h_all = f"{home_name.lower()}|{away_name.lower()}|all"
    h2h_data = h2h.get(h2h_key) or h2h.get(h2h_all)
    if h2h_data and h2h_data.get("matches_played", 0) >= 2:
        h_h2h = h2h_data.get("win_rate", 0.5)
        a_h2h = 1 - h_h2h
        h2h_weight = 0.10
    else:
        h_h2h, a_h2h = 0.5, 0.5
        h2h_weight = 0.0

    # Redistribute H2H weight if no data
    form_w = 0.35 + (0.10 - h2h_weight) * 0.35
    surf_w = 0.25 + (0.10 - h2h_weight) * 0.25
    recent_w = 0.20 + (0.10 - h2h_weight) * 0.20
    rank_w = 0.10 + (0.10 - h2h_weight) * 0.10

    h_raw = form_w * h_form + surf_w * h_surf + recent_w * h_recent + rank_w * h_rank + h2h_weight * h_h2h
    a_raw = form_w * a_form + surf_w * a_surf + recent_w * a_recent + rank_w * a_rank + h2h_weight * a_h2h

    # Normalize to probabilities
    total = h_raw + a_raw
    if total <= 0:
        return 0.5, 0.5
    return h_raw / total, a_raw / total


def _project_total_games(
    home_sackmann: Optional[Dict[str, Any]],
    away_sackmann: Optional[Dict[str, Any]],
    h2h: Dict[str, Dict[str, Any]],
    home_name: str,
    away_name: str,
    surface: str,
) -> Optional[float]:
    """
    Project total games in the match.

    Uses average of:
      - Each player's avg_games_per_match (surface-specific + recent L5 blend)
      - H2H avg_games_per_match if available
    """
    def _player_avg(s: Optional[Dict]) -> Optional[float]:
        if not s:
            return None
        recent = s.get("recent_avg_games_l5")
        career = s.get("avg_games_per_match")
        if recent and career:
            return recent * 0.6 + career * 0.4  # Weight recent form more
        return recent or career

    h_avg = _player_avg(home_sackmann)
    a_avg = _player_avg(away_sackmann)

    if h_avg is None and a_avg is None:
        return None

    # Individual player averages → match total estimate
    # Each player's avg includes their games in the match, so we average them
    # (not sum, since each player's avg already reflects the full match)
    estimates = []
    if h_avg is not None and a_avg is not None:
        estimates.append((h_avg + a_avg) / 2)
    elif h_avg is not None:
        estimates.append(h_avg)
    elif a_avg is not None:
        estimates.append(a_avg)

    # H2H adjustment
    h2h_key = f"{home_name.lower()}|{away_name.lower()}|{surface}"
    h2h_all = f"{home_name.lower()}|{away_name.lower()}|all"
    h2h_data = h2h.get(h2h_key) or h2h.get(h2h_all)
    if h2h_data and h2h_data.get("avg_games_per_match"):
        estimates.append(h2h_data["avg_games_per_match"])

    if not estimates:
        return None
    return sum(estimates) / len(estimates)


def _project_sets(
    home_analytics: Optional[Dict[str, Any]],
    away_analytics: Optional[Dict[str, Any]],
    home_win_prob: float,
) -> Dict[str, float]:
    """
    Project set-related outcomes.

    Returns:
      - straight_sets_prob: probability match ends in straight sets (either player)
      - home_minus_1_5_sets: probability home wins in straight sets
      - away_minus_1_5_sets: probability away wins in straight sets
    """
    h_ss = (home_analytics or {}).get("straight_sets_rate", 0.4)
    a_ss = (away_analytics or {}).get("straight_sets_rate", 0.4)

    # Probability of straight sets win for each player
    # = P(player wins) × P(player wins in straight sets | player wins)
    h_ss_prob = home_win_prob * (h_ss or 0.4)
    a_ss_prob = (1 - home_win_prob) * (a_ss or 0.4)

    return {
        "straight_sets_prob": h_ss_prob + a_ss_prob,
        "home_minus_1_5_sets": h_ss_prob,
        "away_minus_1_5_sets": a_ss_prob,
    }


def _project_game_spread(
    home_win_prob: float,
    projected_total: Optional[float],
    home_sackmann: Optional[Dict[str, Any]],
    away_sackmann: Optional[Dict[str, Any]],
) -> Optional[float]:
    """
    Project home game spread (negative = home favored).

    Uses win probability and average games data to estimate
    the expected game differential.
    """
    if projected_total is None:
        return None

    # Estimate game share based on win probability and serve strength
    # A player with 60% win prob typically wins ~53-55% of games
    # The relationship is dampened because tennis games are won/lost at service
    game_share = 0.5 + (home_win_prob - 0.5) * 0.3
    home_games = projected_total * game_share
    away_games = projected_total * (1 - game_share)
    return round(home_games - away_games, 1)


# ---------------------------------------------------------------------------
# Main projection builder
# ---------------------------------------------------------------------------

def build_projections(
    event_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Build projections for all (or one) upcoming ATP match with FanDuel odds.

    Returns a list of match projections, each containing:
      - Match metadata (players, tournament, start time)
      - Moneyline projection (win prob, edge vs FanDuel)
      - Total games projection (projected total vs FanDuel lines)
      - Set spread projection (straight sets probability)
      - Game spread projection (projected spread vs FanDuel lines)
      - Individual FanDuel market odds with edge annotations
    """
    odds_rows = _fetch_latest_fanduel_odds()
    if not odds_rows:
        logger.warning("No FanDuel ATP odds found in last 24h")
        return []

    # Group odds by event
    events: Dict[str, Dict[str, Any]] = {}
    for row in odds_rows:
        eid = row["event_id"]
        if event_id and eid != event_id:
            continue
        if eid not in events:
            events[eid] = {
                "event_id": eid,
                "event_name": row["event_name"],
                "tournament_name": row["tournament_name"],
                "player_home": row["player_home"],
                "player_away": row["player_away"],
                "event_start": str(row["event_start"]) if row.get("event_start") else None,
                "is_inplay": row.get("is_inplay", False),
                "markets": {},
            }
        market_name = row["market_name"]
        if market_name not in events[eid]["markets"]:
            events[eid]["markets"][market_name] = []
        events[eid]["markets"][market_name].append(row)

    # Skip in-play matches
    events = {k: v for k, v in events.items() if not v.get("is_inplay")}

    if not events:
        return []

    # Collect all player names for bulk lookup
    all_players = set()
    for ev in events.values():
        home = ev.get("player_home")
        away = ev.get("player_away")
        if home:
            all_players.add(home)
        if away:
            all_players.add(away)

    # Also add full selection names for lookup
    for row in odds_rows:
        sel = row.get("selection_name")
        if sel and "/" not in sel:
            all_players.add(sel)

    player_list = list(all_players)
    analytics = _fetch_analytics_for_players(player_list)
    sackmann = _fetch_sackmann_features(player_list)
    h2h = _fetch_h2h_features(player_list)

    projections = []
    for eid, ev in events.items():
        tournament = ev["tournament_name"] or ""
        surface = _infer_surface(tournament)

        # Find full player names from moneyline selections
        home_short = ev["player_home"] or ""
        away_short = ev["player_away"] or ""
        home_full = home_short
        away_full = away_short

        ml_rows = ev["markets"].get("Moneyline", [])
        for r in ml_rows:
            sel = r.get("selection_name", "")
            if sel.lower().startswith(home_short.lower().split()[0].lower()):
                home_full = sel
            elif sel.lower().startswith(away_short.lower().split()[0].lower()):
                away_full = sel

        # Skip doubles
        if "/" in home_full or "/" in away_full:
            continue

        # Get analytics
        h_analytics = _get_analytics(analytics, home_full, surface)
        a_analytics = _get_analytics(analytics, away_full, surface)

        if not h_analytics and not a_analytics:
            continue

        h_analytics = h_analytics or {}
        a_analytics = a_analytics or {}

        h_sackmann = _get_sackmann(sackmann, home_full, surface)
        a_sackmann = _get_sackmann(sackmann, away_full, surface)

        # --- Moneyline projection ---
        h_prob, a_prob = _project_moneyline(
            h_analytics, a_analytics, surface, h2h, home_full, away_full,
        )

        # FanDuel moneyline odds
        fd_home_odds = None
        fd_away_odds = None
        for r in ml_rows:
            sel = r.get("selection_name", "")
            if sel == home_full:
                fd_home_odds = r
            elif sel == away_full:
                fd_away_odds = r

        ml_projection = {
            "home_win_prob": round(h_prob, 3),
            "away_win_prob": round(a_prob, 3),
            "home_projected_american": _american_from_prob(h_prob),
            "away_projected_american": _american_from_prob(a_prob),
        }

        if fd_home_odds and fd_home_odds.get("odds_american"):
            fd_h_implied = _implied_prob(fd_home_odds["odds_american"])
            ml_projection["home_fd_american"] = fd_home_odds["odds_american"]
            ml_projection["home_fd_implied_prob"] = round(fd_h_implied, 3)
            ml_projection["home_edge"] = round(h_prob - fd_h_implied, 3)
            ml_projection["home_deep_link"] = fd_home_odds.get("deep_link")

        if fd_away_odds and fd_away_odds.get("odds_american"):
            fd_a_implied = _implied_prob(fd_away_odds["odds_american"])
            ml_projection["away_fd_american"] = fd_away_odds["odds_american"]
            ml_projection["away_fd_implied_prob"] = round(fd_a_implied, 3)
            ml_projection["away_edge"] = round(a_prob - fd_a_implied, 3)
            ml_projection["away_deep_link"] = fd_away_odds.get("deep_link")

        # --- Total games projection ---
        projected_total = _project_total_games(
            h_sackmann, a_sackmann, h2h, home_full, away_full, surface,
        )

        total_games_projection = None
        if projected_total:
            total_games_projection = {
                "projected_total": round(projected_total, 1),
            }
            # Compare to FanDuel alt total lines
            # Lines are embedded in selection_name (e.g. "Over 21.5") since
            # the handicap field is always 0 for tennis totals.
            alt_totals = []
            for market_name, rows in ev["markets"].items():
                if "Alternative Total Games" not in market_name:
                    continue
                for r in rows:
                    sel = (r.get("selection_name") or "")
                    sel_lower = sel.lower()
                    is_over = sel_lower.startswith("over")
                    is_under = sel_lower.startswith("under")
                    if not is_over and not is_under:
                        continue
                    # Parse line from selection name: "Over 21.5" → 21.5
                    try:
                        line = float(sel.split()[-1])
                    except (ValueError, IndexError):
                        continue
                    alt_totals.append({
                        "line": line,
                        "side": "over" if is_over else "under",
                        "odds_american": r.get("odds_american"),
                        "odds_decimal": r.get("odds_decimal"),
                        "deep_link": r.get("deep_link"),
                        "market_name": market_name,
                    })

            # Find edges on total lines
            total_edges = []
            for t in alt_totals:
                line = t["line"]
                diff = projected_total - line
                if t["side"] == "over" and diff > 0.5:
                    total_edges.append({**t, "edge_games": round(diff, 1), "direction": "OVER"})
                elif t["side"] == "under" and diff < -0.5:
                    total_edges.append({**t, "edge_games": round(abs(diff), 1), "direction": "UNDER"})

            total_edges.sort(key=lambda x: x["edge_games"], reverse=True)
            total_games_projection["edges"] = total_edges[:6]

        # --- Set spread projection ---
        set_proj = _project_sets(h_analytics, a_analytics, h_prob)
        set_spread_projection = {
            "straight_sets_prob": round(set_proj["straight_sets_prob"], 3),
            "home_straight_sets_prob": round(set_proj["home_minus_1_5_sets"], 3),
            "away_straight_sets_prob": round(set_proj["away_minus_1_5_sets"], 3),
        }

        # Compare to FanDuel set spread lines
        # Selection names: "Alex Michelsen (-1.5)" or "Coleman Wong (+1.5)"
        set_spread_rows = ev["markets"].get("Alternative Set Spread", [])
        for r in set_spread_rows:
            sel = (r.get("selection_name") or "")
            odds_am = r.get("odds_american")
            if odds_am is None:
                continue
            line_match = re.search(r'\(([+-]?\d+\.?\d*)\)', sel)
            if not line_match:
                continue
            line_val = float(line_match.group(1))
            player_part = sel[:sel.rfind("(")].strip()
            fd_implied = _implied_prob(odds_am)

            if line_val == -1.5:
                # -1.5 sets = win in straight sets
                if player_part == home_full:
                    model_prob = set_proj["home_minus_1_5_sets"]
                    set_spread_projection["home_minus_1_5_fd_american"] = odds_am
                    set_spread_projection["home_minus_1_5_fd_implied"] = round(fd_implied, 3)
                    set_spread_projection["home_minus_1_5_edge"] = round(model_prob - fd_implied, 3)
                    set_spread_projection["home_minus_1_5_deep_link"] = r.get("deep_link")
                elif player_part == away_full:
                    model_prob = set_proj["away_minus_1_5_sets"]
                    set_spread_projection["away_minus_1_5_fd_american"] = odds_am
                    set_spread_projection["away_minus_1_5_fd_implied"] = round(fd_implied, 3)
                    set_spread_projection["away_minus_1_5_edge"] = round(model_prob - fd_implied, 3)
                    set_spread_projection["away_minus_1_5_deep_link"] = r.get("deep_link")

        # --- Game spread projection ---
        projected_spread = _project_game_spread(h_prob, projected_total, h_sackmann, a_sackmann)
        game_spread_projection = None
        if projected_spread is not None:
            game_spread_projection = {
                "projected_home_spread": projected_spread,
            }
            # Compare to FanDuel game spread lines
            # Selection names look like "Alex Michelsen (-4.5)" or "Coleman Wong (+2.5)"
            gs_rows = ev["markets"].get("Alternative Game Spread", [])
            gs_edges = []
            for r in gs_rows:
                sel = (r.get("selection_name") or "")
                odds_am = r.get("odds_american")
                if odds_am is None:
                    continue
                # Parse line from selection name: "Player Name (-4.5)" → -4.5
                line_match = re.search(r'\(([+-]?\d+\.?\d*)\)', sel)
                if not line_match:
                    continue
                h = float(line_match.group(1))
                player_part = sel[:sel.rfind("(")].strip()

                if player_part == home_full:
                    # Projected spread is from home perspective (negative = home favored)
                    # If projected spread is -2.0 and line is -4.5, model says home
                    # covers by 2.5 fewer games than the line requires
                    # Edge = projected_spread - line (more negative projected = better for home)
                    edge = h - projected_spread  # positive = model says home covers
                    if edge > 0.5:
                        gs_edges.append({
                            "player": home_full,
                            "line": h,
                            "edge_games": round(edge, 1),
                            "odds_american": odds_am,
                            "deep_link": r.get("deep_link"),
                        })
                elif player_part == away_full:
                    # Away spread: if projected spread is -2.0 (home favored by 2),
                    # and line is +4.5, away covers if actual margin < 4.5
                    edge = (h + projected_spread)  # positive = model says away covers
                    if edge > 0.5:
                        gs_edges.append({
                            "player": away_full,
                            "line": h,
                            "edge_games": round(edge, 1),
                            "odds_american": odds_am,
                            "deep_link": r.get("deep_link"),
                        })
            gs_edges.sort(key=lambda x: x["edge_games"], reverse=True)
            game_spread_projection["edges"] = gs_edges[:6]

        # --- Build match projection ---
        # Determine best edge across all markets
        best_edge = None
        edges_list = []
        if ml_projection.get("home_edge"):
            edges_list.append(("Moneyline", home_full, ml_projection["home_edge"]))
        if ml_projection.get("away_edge"):
            edges_list.append(("Moneyline", away_full, ml_projection["away_edge"]))

        if edges_list:
            edges_list.sort(key=lambda x: x[2], reverse=True)
            best = edges_list[0]
            if best[2] >= 0.03:
                best_edge = {
                    "market": best[0],
                    "player": best[1],
                    "edge": best[2],
                    "label": "Strong Edge" if best[2] >= 0.08 else "Edge" if best[2] >= 0.05 else "Lean",
                }

        projections.append({
            "event_id": eid,
            "event_name": ev["event_name"],
            "tournament_name": tournament,
            "surface": surface,
            "player_home": home_full,
            "player_away": away_full,
            "event_start": ev["event_start"],
            "home_rank": h_analytics.get("world_rank"),
            "away_rank": a_analytics.get("world_rank"),
            "home_form_score": h_analytics.get("betting_form_score"),
            "away_form_score": a_analytics.get("betting_form_score"),
            "home_streak": h_analytics.get("current_win_streak", 0) or -(h_analytics.get("current_loss_streak", 0)),
            "away_streak": a_analytics.get("current_win_streak", 0) or -(a_analytics.get("current_loss_streak", 0)),
            "moneyline": ml_projection,
            "total_games": total_games_projection,
            "set_spread": set_spread_projection,
            "game_spread": game_spread_projection,
            "best_edge": best_edge,
        })

    projections.sort(key=lambda x: x.get("event_start") or "")
    return projections
