# routes/correlations.py
"""
Prop Correlations API – surfaces teammate prop pairs that tend to hit together.

Uses game_advanced_stats to compute statistical correlations between
teammate performance metrics over recent shared games, then maps those
correlations to tonight's active prop markets.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional
import math

from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client
from managed_live_ingest import nba_today

router = APIRouter(prefix="/analytics", tags=["Analytics"])


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _pearson(xs: List[float], ys: List[float]) -> Optional[float]:
    """Pearson correlation coefficient. Returns None if insufficient data."""
    n = len(xs)
    if n < 5 or len(ys) != n:
        return None

    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))

    if den_x == 0 or den_y == 0:
        return None

    return num / (den_x * den_y)


def _both_over_rate(
    xs: List[float],
    ys: List[float],
    x_avg: float,
    y_avg: float,
) -> Optional[float]:
    """Fraction of games where both players exceeded their averages."""
    n = len(xs)
    if n < 5 or len(ys) != n:
        return None

    both_over = sum(1 for x, y in zip(xs, ys) if x >= x_avg and y >= y_avg)
    return both_over / n


def _correlation_strength(r: Optional[float]) -> str:
    if r is None:
        return "insufficient"
    abs_r = abs(r)
    if abs_r >= 0.60:
        return "strong"
    if abs_r >= 0.35:
        return "moderate"
    if abs_r >= 0.20:
        return "weak"
    return "negligible"


def _map_metric_to_markets(metric: str) -> List[str]:
    """Map an advanced metric to the prop markets it best predicts."""
    mapping = {
        "usage_percentage": ["pts", "fga"],
        "offensive_rating": ["pts", "pra"],
        "assist_ratio": ["ast", "pa"],
        "rebound_percentage": ["reb", "pr", "pra"],
        "pace": ["pts", "reb", "ast", "pra"],
        "pie": ["pts", "reb", "ast", "pra"],
    }
    return mapping.get(metric, ["pts"])


def _generate_insight(
    player_a: str,
    player_b: str,
    metric: str,
    corr: float,
    both_rate: Optional[float],
) -> str:
    """Generate a human-readable insight for the correlation."""
    direction = "rise" if corr > 0 else "drop"
    abs_corr = abs(corr)

    if metric == "usage_percentage":
        if corr > 0:
            return (
                f"When {player_a} has high usage, {player_b} also sees "
                f"increased volume — they feed off each other's energy"
            )
        return (
            f"When {player_a} dominates usage, {player_b}'s volume tends to "
            f"decrease — consider unders if {player_a} is hot"
        )

    if metric == "offensive_rating":
        if corr > 0:
            return (
                f"{player_a} and {player_b} both perform well in the same games — "
                f"a strong same-game parlay pair"
            )
        return (
            f"{player_a} and {player_b} tend to alternate big games — "
            f"avoid pairing their overs"
        )

    if metric == "assist_ratio":
        if corr > 0:
            return (
                f"Both players' assist rates trend together — high ball movement "
                f"games boost both"
            )
        return (
            f"One player's passing tends to come at the other's expense"
        )

    if metric == "pie":
        if corr > 0:
            return (
                f"{player_a} and {player_b} have linked impact — when one "
                f"performs well, so does the other ({abs_corr:.0%} correlation)"
            )
        return (
            f"Their impact alternates — parlay one's over with the other's under"
        )

    return (
        f"{player_a} and {player_b} show a {_correlation_strength(corr)} "
        f"{'positive' if corr > 0 else 'negative'} correlation in {metric}"
    )


def serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# ──────────────────────────────────────────────
# Endpoint
# ──────────────────────────────────────────────

@router.get("/correlations")
def get_prop_correlations(
    game_date: Optional[date] = Query(None),
    min_games: int = Query(5, ge=3, le=20),
    lookback: int = Query(15, ge=5, le=30),
    limit: int = Query(50, ge=1, le=200),
):
    """
    Compute teammate prop correlations for tonight's games.

    Uses game_advanced_stats to find statistical links between players
    on the same team over their last N shared games.
    """
    query_date = game_date or nba_today()
    client = get_bq_client()

    # ── Step 1: Get tonight's games ──────────────────────────
    # Try the betting view first, fall back to the games table
    games_rows = []
    game_source = "betting"

    try:
        games_query = """
        SELECT DISTINCT
            game_id,
            home_team_abbr,
            away_team_abbr
        FROM `nba_goat_data.v_game_betting_base`
        WHERE game_date = @game_date
          AND (is_final IS NULL OR is_final = FALSE)
        """
        games_rows = list(
            client.query(
                games_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("game_date", "DATE", query_date),
                    ]
                ),
            ).result()
        )
    except Exception as e:
        print(f"[CORRELATIONS] Betting view query failed: {e}")

    # Fallback: try the games table directly
    if not games_rows:
        try:
            fallback_query = """
            SELECT DISTINCT
                game_id,
                home_team_abbr,
                away_team_abbr
            FROM `nba_goat_data.games`
            WHERE game_date = @game_date
              AND (is_final IS NULL OR is_final = FALSE)
            """
            games_rows = list(
                client.query(
                    fallback_query,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("game_date", "DATE", query_date),
                        ]
                    ),
                ).result()
            )
            game_source = "games_table"
        except Exception as e:
            print(f"[CORRELATIONS] Games table fallback also failed: {e}")

    if not games_rows:
        return {
            "game_date": query_date.isoformat(),
            "count": 0,
            "games": [],
        }

    # Collect all team abbreviations playing tonight
    team_abbrs = set()
    game_teams: Dict[int, Dict] = {}
    for g in games_rows:
        gd = dict(g)
        game_teams[gd["game_id"]] = gd
        team_abbrs.add(gd["home_team_abbr"])
        team_abbrs.add(gd["away_team_abbr"])

    # ── Step 2: Get recent game stats for players on tonight's teams ──
    team_list = ", ".join(f"'{t}'" for t in sorted(team_abbrs))

    try:
        stats_query = f"""
        WITH recent AS (
            SELECT
                player_id,
                player_first_name,
                player_last_name,
                team_abbreviation,
                game_id,
                game_date,
                usage_percentage,
                offensive_rating,
                assist_ratio,
                rebound_percentage,
                pace,
                pie,
                ROW_NUMBER() OVER (
                    PARTITION BY player_id
                    ORDER BY game_date DESC
                ) AS game_num
            FROM `nba_live.game_advanced_stats`
            WHERE period = 0
              AND team_abbreviation IN ({team_list})
              AND usage_percentage IS NOT NULL
              AND game_date < @game_date
        )
        SELECT *
        FROM recent
        WHERE game_num <= @lookback
        ORDER BY team_abbreviation, player_id, game_date DESC
        """

        stats_rows = list(
            client.query(
                stats_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("game_date", "DATE", query_date),
                        bigquery.ScalarQueryParameter("lookback", "INT64", lookback),
                    ]
                ),
            ).result()
        )
    except Exception as e:
        print(f"[CORRELATIONS] Game advanced stats query failed: {e}")
        return {
            "game_date": query_date.isoformat(),
            "count": 0,
            "games": [],
            "error": "Could not load game advanced stats",
        }

    if not stats_rows:
        return {
            "game_date": query_date.isoformat(),
            "count": 0,
            "games": [],
        }

    # ── Step 3: Organize stats by team -> player -> game list ──
    team_players: Dict[str, Dict[int, Dict]] = {}
    for row in stats_rows:
        r = dict(row)
        team = r["team_abbreviation"]
        pid = r["player_id"]

        if team not in team_players:
            team_players[team] = {}

        if pid not in team_players[team]:
            team_players[team][pid] = {
                "name": f"{r['player_first_name']} {r['player_last_name']}",
                "first_name": r["player_first_name"],
                "last_name": r["player_last_name"],
                "games": {},
            }

        team_players[team][pid]["games"][r["game_id"]] = {
            "usage_percentage": r.get("usage_percentage"),
            "offensive_rating": r.get("offensive_rating"),
            "assist_ratio": r.get("assist_ratio"),
            "rebound_percentage": r.get("rebound_percentage"),
            "pace": r.get("pace"),
            "pie": r.get("pie"),
        }

    # ── Step 4: Get tonight's props for market context (optional) ──
    player_markets: Dict[int, List[Dict]] = {}
    try:
        props_query = """
        SELECT DISTINCT
            player_id,
            market_key,
            line_value
        FROM `nba_live.player_prop_odds_master_staging`
        WHERE market_window = 'FULL'
        """
        props_rows = list(client.query(props_query).result())

        for pr in props_rows:
            prd = dict(pr)
            pid = prd["player_id"]
            if pid not in player_markets:
                player_markets[pid] = []
            player_markets[pid].append({
                "market": prd.get("market_key"),
                "line": prd.get("line_value"),
            })
    except Exception as e:
        print(f"[CORRELATIONS] Props query failed (non-fatal): {e}")
        # Continue without prop market data — correlations still work

    # ── Step 5: Compute correlations for teammate pairs ──
    metrics = ["usage_percentage", "offensive_rating", "assist_ratio", "pie"]
    all_correlations: List[Dict] = []
    has_props = len(player_markets) > 0

    for team_abbr, players in team_players.items():
        player_ids = sorted(players.keys())

        for i in range(len(player_ids)):
            for j in range(i + 1, len(player_ids)):
                pid_a = player_ids[i]
                pid_b = player_ids[j]

                pa = players[pid_a]
                pb = players[pid_b]

                # If we have props data, require at least one player to have props.
                # If props failed to load, include all pairs.
                if has_props:
                    if pid_a not in player_markets and pid_b not in player_markets:
                        continue

                # Find shared games
                shared_games = sorted(
                    set(pa["games"].keys()) & set(pb["games"].keys())
                )

                if len(shared_games) < min_games:
                    continue

                # Compute correlations for each metric
                best_corr = None
                best_metric = None
                best_both_rate = None
                best_abs = 0.0

                for metric in metrics:
                    vals_a = [
                        pa["games"][gid].get(metric)
                        for gid in shared_games
                    ]
                    vals_b = [
                        pb["games"][gid].get(metric)
                        for gid in shared_games
                    ]

                    # Filter out None values
                    pairs = [
                        (a, b) for a, b in zip(vals_a, vals_b)
                        if a is not None and b is not None
                    ]

                    if len(pairs) < min_games:
                        continue

                    xs = [p[0] for p in pairs]
                    ys = [p[1] for p in pairs]

                    r = _pearson(xs, ys)
                    if r is not None and abs(r) > best_abs:
                        best_abs = abs(r)
                        best_corr = r
                        best_metric = metric

                        avg_x = sum(xs) / len(xs)
                        avg_y = sum(ys) / len(ys)
                        best_both_rate = _both_over_rate(xs, ys, avg_x, avg_y)

                if best_corr is None or best_abs < 0.20:
                    continue

                strength = _correlation_strength(best_corr)

                # Find the game_id for this team tonight
                tonight_game_id = None
                for gid, ginfo in game_teams.items():
                    if (
                        ginfo["home_team_abbr"] == team_abbr
                        or ginfo["away_team_abbr"] == team_abbr
                    ):
                        tonight_game_id = gid
                        break

                all_correlations.append({
                    "game_id": tonight_game_id,
                    "team_abbr": team_abbr,
                    "player_a_id": pid_a,
                    "player_a_name": pa["name"],
                    "player_a_markets": player_markets.get(pid_a, []),
                    "player_b_id": pid_b,
                    "player_b_name": pb["name"],
                    "player_b_markets": player_markets.get(pid_b, []),
                    "correlation_metric": best_metric,
                    "correlation_coefficient": round(best_corr, 3),
                    "correlation_strength": strength,
                    "direction": "positive" if best_corr > 0 else "negative",
                    "both_over_rate": (
                        round(best_both_rate, 3) if best_both_rate is not None else None
                    ),
                    "shared_games": len(shared_games),
                    "relevant_markets": _map_metric_to_markets(best_metric),
                    "insight": _generate_insight(
                        pa["first_name"],
                        pb["first_name"],
                        best_metric,
                        best_corr,
                        best_both_rate,
                    ),
                })

    # Sort by absolute correlation strength descending
    all_correlations.sort(key=lambda x: abs(x["correlation_coefficient"]), reverse=True)
    all_correlations = all_correlations[:limit]

    # Group by game for the response
    games_map: Dict[int, Dict] = {}
    for corr in all_correlations:
        gid = corr["game_id"]
        if gid not in games_map:
            ginfo = game_teams.get(gid, {})
            games_map[gid] = {
                "game_id": gid,
                "home_team_abbr": ginfo.get("home_team_abbr"),
                "away_team_abbr": ginfo.get("away_team_abbr"),
                "correlations": [],
            }
        games_map[gid]["correlations"].append(corr)

    games_list = sorted(games_map.values(), key=lambda g: g["game_id"] or 0)

    return {
        "game_date": query_date.isoformat(),
        "count": len(all_correlations),
        "games": games_list,
    }
