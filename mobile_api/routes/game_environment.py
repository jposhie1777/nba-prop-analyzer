# routes/game_environment.py
"""
Game Environment API – contextualizes tonight's games with pace, totals,
rest, and scoring environment data to help evaluate prop value.

Combines Vegas lines, team pace, back-to-back detection, and scoring
trends into a single environment profile per game.
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client
from managed_live_ingest import nba_today

router = APIRouter(prefix="/analytics", tags=["Analytics"])


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _environment_tier(projected_total: Optional[float]) -> str:
    """Classify the game environment based on projected total."""
    if projected_total is None:
        return "UNKNOWN"
    if projected_total >= 235:
        return "SHOOTOUT"
    if projected_total >= 225:
        return "HIGH"
    if projected_total >= 215:
        return "ABOVE_AVG"
    if projected_total >= 210:
        return "AVERAGE"
    if projected_total >= 205:
        return "BELOW_AVG"
    return "GRIND"


def _tier_color(tier: str) -> str:
    """Return a color hint for the environment tier."""
    return {
        "SHOOTOUT": "success",
        "HIGH": "success",
        "ABOVE_AVG": "info",
        "AVERAGE": "warning",
        "BELOW_AVG": "warning",
        "GRIND": "danger",
        "UNKNOWN": "muted",
    }.get(tier, "muted")


def _blowout_risk(spread: Optional[float]) -> Dict[str, Any]:
    """Assess blowout risk based on point spread."""
    if spread is None:
        return {"level": "unknown", "score": None, "label": "No line"}

    abs_spread = abs(spread)
    if abs_spread >= 12:
        return {"level": "high", "score": round(abs_spread, 1), "label": "High blowout risk"}
    if abs_spread >= 7:
        return {"level": "moderate", "score": round(abs_spread, 1), "label": "Some blowout risk"}
    if abs_spread >= 4:
        return {"level": "low", "score": round(abs_spread, 1), "label": "Competitive game"}
    return {"level": "minimal", "score": round(abs_spread, 1), "label": "Toss-up game"}


def _pace_rank_label(rank: Optional[int]) -> str:
    """Human-readable pace ranking."""
    if rank is None:
        return "N/A"
    if rank <= 5:
        return f"#{rank} (Elite)"
    if rank <= 10:
        return f"#{rank} (Fast)"
    if rank <= 15:
        return f"#{rank} (Above Avg)"
    if rank <= 20:
        return f"#{rank} (Average)"
    if rank <= 25:
        return f"#{rank} (Slow)"
    return f"#{rank} (Very Slow)"


def _generate_stat_impacts(
    env_tier: str,
    blowout_level: str,
    combined_pace: Optional[float],
    home_b2b: bool,
    away_b2b: bool,
) -> List[str]:
    """Generate actionable stat impact callouts."""
    impacts = []

    if env_tier in ("SHOOTOUT", "HIGH"):
        impacts.append("PTS/AST overs favored in high-scoring environment")
    elif env_tier in ("GRIND", "BELOW_AVG"):
        impacts.append("Unders more likely in low-scoring environment")

    if combined_pace is not None and combined_pace >= 100:
        impacts.append("Fast pace boosts counting stats across the board")
    elif combined_pace is not None and combined_pace <= 96:
        impacts.append("Slow pace limits possessions — fewer opportunities for everyone")

    if blowout_level == "high":
        impacts.append("Blowout risk: starters may sit Q4 — watch minutes props")
    elif blowout_level == "minimal":
        impacts.append("Close game expected — starters play full minutes")

    if home_b2b:
        impacts.append("Home team on back-to-back: watch for rest, lower energy")
    if away_b2b:
        impacts.append("Away team on back-to-back: fatigue + travel factor")

    if not impacts:
        impacts.append("Standard game environment — no major adjustments needed")

    return impacts


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

@router.get("/game-environment")
def get_game_environment(
    game_date: Optional[date] = Query(None),
    limit: int = Query(20, ge=1, le=50),
):
    """
    Game environment analysis for tonight's slate.

    Combines Vegas lines, team pace, back-to-back detection, and scoring
    trends into a single contextual environment profile per game.
    """
    query_date = game_date or nba_today()
    client = get_bq_client()

    # ── Step 1: Get tonight's games with betting lines ──
    games_query = """
    SELECT
        game_id,
        game_date,
        start_time_est,
        status,
        home_team_abbr,
        away_team_abbr,
        home_moneyline,
        away_moneyline,
        spread_home,
        spread_away,
        total_line
    FROM `nba_goat_data.v_game_betting_base`
    WHERE game_date = @game_date
      AND (is_final IS NULL OR is_final = FALSE)
    ORDER BY start_time_est
    LIMIT @limit
    """

    games_rows = list(
        client.query(
            games_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("game_date", "DATE", query_date),
                    bigquery.ScalarQueryParameter("limit", "INT64", limit),
                ]
            ),
        ).result()
    )

    if not games_rows:
        return {
            "game_date": query_date.isoformat(),
            "count": 0,
            "games": [],
        }

    games = [dict(g) for g in games_rows]

    # Collect team abbreviations
    team_abbrs = set()
    for g in games:
        team_abbrs.add(g["home_team_abbr"])
        team_abbrs.add(g["away_team_abbr"])

    team_list = ", ".join(f"'{t}'" for t in sorted(team_abbrs))

    # ── Step 2: Get team pace + scoring from season averages ──
    pace_query = f"""
    WITH latest AS (
        SELECT
            team_abbreviation,
            CAST(JSON_VALUE(stats, '$.gp') AS INT64) AS gp,
            CAST(JSON_VALUE(stats, '$.pts') AS FLOAT64) AS pts_avg,
            CAST(JSON_VALUE(stats, '$.reb') AS FLOAT64) AS reb_avg,
            CAST(JSON_VALUE(stats, '$.ast') AS FLOAT64) AS ast_avg,
            CAST(JSON_VALUE(stats, '$.tov') AS FLOAT64) AS tov_avg,
            CAST(JSON_VALUE(stats, '$.fg_pct') AS FLOAT64) AS fg_pct,
            CAST(JSON_VALUE(stats, '$.fg3_pct') AS FLOAT64) AS fg3_pct,
            RANK() OVER (ORDER BY CAST(JSON_VALUE(stats, '$.pts') AS FLOAT64) DESC) AS scoring_rank
        FROM `nba_live.team_season_averages`
        WHERE category = 'general'
          AND stat_type = 'base'
          AND season_type = 'regular'
          AND team_abbreviation IN ({team_list})
          AND run_ts = (
              SELECT MAX(run_ts)
              FROM `nba_live.team_season_averages`
              WHERE category = 'general'
                AND stat_type = 'base'
                AND season_type = 'regular'
          )
    )
    SELECT * FROM latest
    """

    pace_rows = list(client.query(pace_query).result())
    team_stats: Dict[str, Dict] = {}
    for row in pace_rows:
        rd = dict(row)
        team_stats[rd["team_abbreviation"]] = rd

    # ── Step 3: Get team pace from advanced stats (actual pace metric) ──
    adv_pace_query = f"""
    WITH team_pace AS (
        SELECT
            team_abbreviation,
            AVG(pace) AS avg_pace,
            STDDEV(pace) AS pace_std,
            COUNT(*) AS games_sampled
        FROM `nba_live.game_advanced_stats`
        WHERE period = 0
          AND team_abbreviation IN ({team_list})
          AND pace IS NOT NULL
          AND game_date >= DATE_SUB(@game_date, INTERVAL 30 DAY)
        GROUP BY team_abbreviation
    ),
    ranked AS (
        SELECT
            *,
            RANK() OVER (ORDER BY avg_pace DESC) AS pace_rank
        FROM team_pace
    )
    SELECT * FROM ranked
    """

    adv_pace_rows = list(
        client.query(
            adv_pace_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("game_date", "DATE", query_date),
                ]
            ),
        ).result()
    )
    team_pace: Dict[str, Dict] = {}
    for row in adv_pace_rows:
        rd = dict(row)
        team_pace[rd["team_abbreviation"]] = rd

    # ── Step 4: Detect back-to-backs ──
    yesterday = query_date - timedelta(days=1)
    b2b_query = f"""
    SELECT DISTINCT
        home_team_abbr AS team_abbr
    FROM `nba_goat_data.games`
    WHERE game_date = @yesterday
      AND is_final = TRUE
      AND home_team_abbr IN ({team_list})
    UNION DISTINCT
    SELECT DISTINCT
        away_team_abbr AS team_abbr
    FROM `nba_goat_data.games`
    WHERE game_date = @yesterday
      AND is_final = TRUE
      AND away_team_abbr IN ({team_list})
    """

    b2b_rows = list(
        client.query(
            b2b_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("yesterday", "DATE", yesterday),
                ]
            ),
        ).result()
    )
    b2b_teams = {dict(r)["team_abbr"] for r in b2b_rows}

    # ── Step 5: Get days since last game for each team ──
    rest_query = f"""
    WITH last_games AS (
        SELECT
            team_abbr,
            MAX(game_date) AS last_game_date
        FROM (
            SELECT home_team_abbr AS team_abbr, game_date
            FROM `nba_goat_data.games`
            WHERE is_final = TRUE AND game_date < @game_date
            UNION ALL
            SELECT away_team_abbr AS team_abbr, game_date
            FROM `nba_goat_data.games`
            WHERE is_final = TRUE AND game_date < @game_date
        )
        WHERE team_abbr IN ({team_list})
        GROUP BY team_abbr
    )
    SELECT
        team_abbr,
        last_game_date,
        DATE_DIFF(@game_date, last_game_date, DAY) AS rest_days
    FROM last_games
    """

    rest_rows = list(
        client.query(
            rest_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("game_date", "DATE", query_date),
                ]
            ),
        ).result()
    )
    team_rest: Dict[str, int] = {}
    for row in rest_rows:
        rd = dict(row)
        team_rest[rd["team_abbr"]] = rd.get("rest_days", 1)

    # ── Step 6: Assemble environment profiles ──
    result_games = []

    for g in games:
        home = g["home_team_abbr"]
        away = g["away_team_abbr"]

        home_stats = team_stats.get(home, {})
        away_stats = team_stats.get(away, {})
        home_pace_data = team_pace.get(home, {})
        away_pace_data = team_pace.get(away, {})

        # Pace
        home_avg_pace = home_pace_data.get("avg_pace")
        away_avg_pace = away_pace_data.get("avg_pace")
        combined_pace = None
        if home_avg_pace is not None and away_avg_pace is not None:
            combined_pace = round((home_avg_pace + away_avg_pace) / 2, 1)

        # Total
        vegas_total = g.get("total_line")

        # Projected total from team scoring averages
        home_pts = home_stats.get("pts_avg")
        away_pts = away_stats.get("pts_avg")
        projected_total = None
        if home_pts is not None and away_pts is not None:
            projected_total = round(home_pts + away_pts, 1)

        # Use vegas total if available, fallback to projected
        env_total = vegas_total or projected_total

        # Environment tier
        tier = _environment_tier(env_total)

        # Blowout risk
        blowout = _blowout_risk(g.get("spread_home"))

        # Rest
        home_b2b = home in b2b_teams
        away_b2b = away in b2b_teams
        home_rest = team_rest.get(home, 1)
        away_rest = team_rest.get(away, 1)

        # Stat impacts
        impacts = _generate_stat_impacts(
            tier,
            blowout["level"],
            combined_pace,
            home_b2b,
            away_b2b,
        )

        result_games.append(serialize_row({
            "game_id": g["game_id"],
            "game_date": g["game_date"],
            "start_time_est": g.get("start_time_est"),
            "home_team_abbr": home,
            "away_team_abbr": away,

            # Vegas lines
            "vegas_total": vegas_total,
            "spread_home": g.get("spread_home"),
            "home_moneyline": g.get("home_moneyline"),
            "away_moneyline": g.get("away_moneyline"),

            # Pace
            "home_pace": round(home_avg_pace, 1) if home_avg_pace else None,
            "away_pace": round(away_avg_pace, 1) if away_avg_pace else None,
            "combined_pace": combined_pace,
            "home_pace_rank": home_pace_data.get("pace_rank"),
            "away_pace_rank": away_pace_data.get("pace_rank"),
            "home_pace_label": _pace_rank_label(home_pace_data.get("pace_rank")),
            "away_pace_label": _pace_rank_label(away_pace_data.get("pace_rank")),

            # Scoring
            "home_pts_avg": round(home_pts, 1) if home_pts else None,
            "away_pts_avg": round(away_pts, 1) if away_pts else None,
            "projected_total": projected_total,

            # Environment
            "environment_tier": tier,
            "environment_color": _tier_color(tier),

            # Blowout risk
            "blowout_risk": blowout,

            # Rest & B2B
            "home_b2b": home_b2b,
            "away_b2b": away_b2b,
            "home_rest_days": home_rest,
            "away_rest_days": away_rest,

            # Impacts
            "stat_impacts": impacts,
        }))

    return {
        "game_date": query_date.isoformat(),
        "count": len(result_games),
        "games": result_games,
    }
