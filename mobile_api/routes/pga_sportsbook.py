"""
PGA Sportsbook Routes — serves FanDuel odds from raw_fanduel_pga_markets,
enriched with analytics from pga_data tables.

Endpoints:
  GET /pga/sportsbook/tournaments  — list available tournaments (distinct event_name)
  GET /pga/sportsbook/markets      — all markets for a tournament, grouped by market_type
"""
from __future__ import annotations

import os
from collections import defaultdict
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(tags=["PGA Sportsbook"])

PROJECT = os.getenv("GCP_PROJECT", "graphite-flare-477419-h7")
RAW_TABLE = f"{PROJECT}.sportsbook.raw_fanduel_pga_markets"
SKILL_STATS_TABLE = f"{PROJECT}.pga_data.website_player_skill_stats"
RECENT_FORM_TABLE = f"{PROJECT}.pga_data.website_player_recent_form"
COURSE_FIT_TABLE = f"{PROJECT}.pga_data.website_course_fit"
BETTING_PROFILE_TABLE = f"{PROJECT}.pga_data.website_player_betting_profile"
SCORECARD_TABLE = f"{PROJECT}.pga_data.website_player_scorecard"

# ── Orphan tournament consolidation ──────────────────────────────────────────
# FanDuel lists sub-markets (3 Balls, Make/Miss Cut, etc.) as separate
# "tournament" entries rather than embedding them in the main PGA weekly event.
# We consolidate them back at query time.

_ORPHAN_NAMES = frozenset({
    "3 Balls", "Make/Miss Cut", "Player Round Scores", "Specials", "Top Region",
    "2 Balls", "Hole Match Betting",
})

# Prefixes for standalone entries that should never absorb orphan sub-events
# (multi-event specials, etc.)
_STANDALONE_PREFIXES = ("Major Specials",)


def _is_pga_tournament(name: str) -> bool:
    """Return True if *name* looks like a PGA Tour weekly event (including majors)."""
    if not name or name in _ORPHAN_NAMES:
        return False
    if any(name.startswith(p) for p in _STANDALONE_PREFIXES):
        return False
    # Must contain a year digit to be a real tournament
    return any(c.isdigit() for c in name)


def _remap_market_type(source_tourn: str, mtype: str, mname: str) -> str:
    """Assign the correct market_type after orphan consolidation."""
    # Orphan tournament overrides
    if source_tourn == "Top Region":
        return "top_nationality"
    if source_tourn == "Specials":
        return "specials"
    if source_tourn == "Player Round Scores":
        return "player_round_score"
    if source_tourn == "Hole Match Betting":
        return "hole_matchup"
    # 3 Balls / 2 Balls / Make/Miss Cut already have correct types (three_ball / matchup / make_cut)

    # Main-tournament sub-categorisation
    if mtype == "outright_winner" and mname in {
        "Big Guns v The Field", "Three Chances to Win", "Two Chances to Win",
    }:
        return "specials"
    if mtype == "finishing_position":
        return "finish_specials"
    if mtype == "round_score":
        return "round_leader" if mname == "1st Round Leader" else "specials"
    return mtype


# ── Player name normalisation ─────────────────────────────────────────────────
# FanDuel strips diacriticals and sometimes merges/splits name parts.
# We normalise both sides of every JOIN to maximise matches.

def _norm_col(col: str) -> str:
    """Return a SQL expression that lowercases, trims, and strips common
    diacriticals from *col* so FanDuel names match PGA Tour names.
    LOWER runs first so replacements only need lowercase variants."""
    inner = f"LOWER(TRIM({col}))"
    for src, dst in [("å", "a"), ("ø", "o"), ("ö", "o"), ("é", "e"),
                     ("ü", "u"), ("ñ", "n"), ("è", "e"), ("á", "a"),
                     ("í", "i"), ("ó", "o"), ("ú", "u")]:
        inner = f"REPLACE({inner},'{src}','{dst}')"
    return inner


# Explicit aliases for names that differ beyond diacriticals
_FD_TO_PGA_ALIASES: Dict[str, str] = {
    "minwoo lee": "min woo lee",
    "byeong hun an": "byeong-hun an",
    "jj spaun": "j.j. spaun",
    "si woo kim": "si woo kim",
    "cam davis": "cameron davis",
}


def _alias_cte() -> str:
    """Return a CTE that maps FanDuel player names to PGA Tour equivalents."""
    if not _FD_TO_PGA_ALIASES:
        return "name_alias AS (SELECT CAST(NULL AS STRING) fd, CAST(NULL AS STRING) pga WHERE FALSE)"
    unions = " UNION ALL ".join(
        f"SELECT '{fd}' fd, '{pga}' pga" for fd, pga in _FD_TO_PGA_ALIASES.items()
    )
    return f"name_alias AS ({unions})"


def _query(
    sql: str,
    params: Optional[List[Any]] = None,
) -> List[Dict[str, Any]]:
    client = get_bq_client()
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=params or []),
    )
    return [dict(row) for row in job.result()]


def _serialise(row: Dict[str, Any]) -> Dict[str, Any]:
    """Make non-JSON types serialisable."""
    import datetime as _dt

    out = {}
    for k, v in row.items():
        if isinstance(v, (_dt.datetime, _dt.date)):
            out[k] = v.isoformat()
        elif isinstance(v, _dt.timedelta):
            out[k] = v.total_seconds()
        else:
            out[k] = v
    return out


# ---------------------------------------------------------------------------
# GET /pga/sportsbook/tournaments
# ---------------------------------------------------------------------------

@router.get("/pga/sportsbook/tournaments")
def pga_sportsbook_tournaments() -> List[Dict[str, Any]]:
    """
    List all available tournaments from the most recent scrape run.
    Returns one entry per tournament with market counts by type.
    """
    # Step 1: get distinct tournaments
    tourn_sql = f"""
    SELECT
      COALESCE(tournament_name, event_name) AS tournament_name,
      MAX(NULLIF(tournament_slug, ''))      AS tournament_slug,
      MAX(scraped_at)                       AS last_scraped,
      COUNT(*)                              AS total_selections
    FROM `{RAW_TABLE}`
    WHERE scraped_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
      AND market_status IN ('ACTIVE', 'OPEN', 'SUSPENDED')
    GROUP BY 1
    ORDER BY last_scraped DESC
    """
    tourns = _query(tourn_sql)

    # Step 2: get market type counts per tournament
    mt_sql = f"""
    SELECT
      COALESCE(tournament_name, event_name) AS tournament_name,
      market_type,
      COUNT(*) AS selection_count
    FROM `{RAW_TABLE}`
    WHERE scraped_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
      AND market_status IN ('ACTIVE', 'OPEN', 'SUSPENDED')
    GROUP BY 1, 2
    ORDER BY 1, selection_count DESC
    """
    mt_rows = _query(mt_sql)

    # Index market types by tournament name
    mt_by_tourn: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in mt_rows:
        mt_by_tourn[r["tournament_name"]].append(
            {"type": r["market_type"], "count": r["selection_count"]}
        )

    result = []
    for t in tourns:
        entry = _serialise(t)
        entry["market_types"] = mt_by_tourn.get(t["tournament_name"], [])
        result.append(entry)

    # ── Consolidate orphan tournaments into the main PGA event ─────────
    # Pick the real tournament with the most selections (the current week's
    # headliner), not just the first name that pattern-matches.
    pga_candidates = [
        t for t in result if _is_pga_tournament(t.get("tournament_name", ""))
    ]
    pga_candidates.sort(key=lambda t: t.get("total_selections", 0), reverse=True)
    main_pga = pga_candidates[0]["tournament_name"] if pga_candidates else None
    if main_pga:
        main_entry = next(t for t in result if t["tournament_name"] == main_pga)
        for t in list(result):
            if t.get("tournament_name", "") not in _ORPHAN_NAMES:
                continue
            main_entry["total_selections"] += t.get("total_selections", 0)
            for mt in t.get("market_types", []):
                remapped = _remap_market_type(t["tournament_name"], mt["type"], "")
                existing = next(
                    (m for m in main_entry["market_types"] if m["type"] == remapped), None
                )
                if existing:
                    existing["count"] += mt["count"]
                else:
                    main_entry["market_types"].append({"type": remapped, "count": mt["count"]})
        result = [t for t in result if t.get("tournament_name", "") not in _ORPHAN_NAMES]

    return result


# ---------------------------------------------------------------------------
# GET /pga/sportsbook/markets
# ---------------------------------------------------------------------------

@router.get("/pga/sportsbook/markets")
def pga_sportsbook_markets(
    tournament: str = Query(..., description="Tournament name (from /tournaments endpoint)"),
    market_type: Optional[str] = Query(None, description="Filter by market_type (e.g. outright_winner, matchup)"),
) -> Dict[str, Any]:
    """
    Returns all FanDuel odds for a tournament, grouped by market_type,
    enriched with player analytics from pga_data.

    For PGA weekly events the response also includes selections from orphan
    FanDuel "tournaments" (3 Balls, Make/Miss Cut, etc.) with remapped
    market_type values so each bet category gets its own tab.
    """
    # Expand tournament list to include orphan sub-markets for PGA events
    is_pga = _is_pga_tournament(tournament)
    tournament_names = [tournament]
    if is_pga:
        tournament_names.extend(sorted(_ORPHAN_NAMES))

    params: List[Any] = [
        bigquery.ArrayQueryParameter("tournament_names", "STRING", tournament_names),
    ]

    market_filter = ""
    if market_type:
        market_filter = "AND r.market_type = @market_type"
        params.append(
            bigquery.ScalarQueryParameter("market_type", "STRING", market_type),
        )

    # Build normalised-join SQL expression
    fd_norm = _norm_col("COALESCE(na.pga, raw.player_name)")
    sk_norm = _norm_col("sk.player_name")
    rf_norm = _norm_col("rf.player_display_name")
    bp_norm = _norm_col("bp.player_display_name")

    l5_norm = _norm_col("l5r.player_display_name")
    ct_norm = _norm_col("ct.player_display_name")

    sql = f"""
    WITH {_alias_cte()},
    -- Last 5 individual round scores per player
    l5_rounds AS (
      SELECT player_display_name,
        STRING_AGG(CAST(round_score AS STRING), ',' ORDER BY rn) AS l5_round_scores
      FROM (
        SELECT player_display_name, round_score,
          ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY tournament_date DESC, round_num) AS rn
        FROM (
          SELECT DISTINCT player_id, player_display_name, tournament_date,
            r1 AS round_score, 1 AS round_num FROM `{SCORECARD_TABLE}` WHERE r1 IS NOT NULL
          UNION ALL
          SELECT DISTINCT player_id, player_display_name, tournament_date,
            r2, 2 FROM `{SCORECARD_TABLE}` WHERE r2 IS NOT NULL
          UNION ALL
          SELECT DISTINCT player_id, player_display_name, tournament_date,
            r3, 3 FROM `{SCORECARD_TABLE}` WHERE r3 IS NOT NULL
          UNION ALL
          SELECT DISTINCT player_id, player_display_name, tournament_date,
            r4, 4 FROM `{SCORECARD_TABLE}` WHERE r4 IS NOT NULL
        )
      )
      WHERE rn <= 5
      GROUP BY player_display_name
    ),
    -- Current tournament scores (if available) — DISTINCT to avoid dupe scorecard rows
    -- Only show scores when the most recent tournament started within the last
    -- 4 days (covers Thu→Sun of a tournament week). This prevents stale scores
    -- from last week's event bleeding into the current week's view.
    current_tourn AS (
      SELECT DISTINCT player_display_name, position, to_par, r1, r2, r3, r4, total_strokes
      FROM `{SCORECARD_TABLE}`
      WHERE season = EXTRACT(YEAR FROM CURRENT_DATE())
        AND tournament_date = (
          SELECT MAX(tournament_date) FROM `{SCORECARD_TABLE}`
          WHERE season = EXTRACT(YEAR FROM CURRENT_DATE())
        )
        AND tournament_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)
    ),
    latest_per_tourn AS (
      SELECT
        COALESCE(tournament_name, event_name) AS tn,
        MAX(scraped_at) AS ts
      FROM `{RAW_TABLE}`
      WHERE COALESCE(tournament_name, event_name) IN UNNEST(@tournament_names)
        AND scraped_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
      GROUP BY 1
    ),
    raw AS (
      SELECT
        r.market_id,
        r.market_name,
        r.market_type,
        r.market_status,
        r.selection_id,
        r.player_name,
        r.odds_decimal,
        r.odds_american,
        r.handicap,
        r.deep_link,
        r.scraped_at,
        COALESCE(r.tournament_name, r.event_name) AS tournament_name,
        COALESCE(r.tournament_name, r.event_name) AS source_tournament
      FROM `{RAW_TABLE}` r
      JOIN latest_per_tourn lpt
        ON COALESCE(r.tournament_name, r.event_name) = lpt.tn
       AND r.scraped_at = lpt.ts
      WHERE r.market_status IN ('ACTIVE', 'OPEN', 'SUSPENDED')
        {market_filter}
    )
    SELECT
      raw.*,
      -- Skill stats
      sk.sg_total,
      sk.sg_off_tee,
      sk.sg_approach,
      sk.sg_putting,
      sk.sg_around_green,
      sk.sg_tee_to_green,
      sk.driving_distance,
      sk.driving_accuracy,
      sk.gir_pct,
      sk.scrambling_pct,
      sk.scoring_avg,
      sk.birdie_avg,
      sk.par3_scoring_avg,
      sk.par4_scoring_avg,
      sk.par5_scoring_avg,
      sk.bounce_back_pct,
      sk.putts_per_gir,
      sk.proximity_to_hole,
      -- Recent form
      rf.l5_total_score_avg,
      rf.l5_finish_avg,
      rf.cut_rate_l5,
      rf.top10_rate_l5,
      rf.weighted_l5_score,
      rf.form_trend_3,
      rf.days_since_last_event,
      rf.score_stddev,
      -- Betting profile
      bp.season_total_score_avg,
      bp.season_finish_avg,
      -- Last 5 round scores
      l5r.l5_round_scores,
      -- Current tournament
      ct.position  AS current_position,
      ct.to_par    AS current_to_par,
      ct.r1        AS current_r1,
      ct.r2        AS current_r2,
      ct.r3        AS current_r3,
      ct.r4        AS current_r4
    FROM raw
    LEFT JOIN name_alias na ON LOWER(TRIM(raw.player_name)) = na.fd
    LEFT JOIN `{SKILL_STATS_TABLE}` sk
      ON {fd_norm} = {sk_norm}
    LEFT JOIN `{RECENT_FORM_TABLE}` rf
      ON {fd_norm} = {rf_norm}
    LEFT JOIN `{BETTING_PROFILE_TABLE}` bp
      ON {fd_norm} = {bp_norm}
    LEFT JOIN l5_rounds l5r
      ON {fd_norm} = {l5_norm}
    LEFT JOIN current_tourn ct
      ON {fd_norm} = {ct_norm}
    ORDER BY raw.market_type, raw.market_name, raw.odds_decimal ASC NULLS LAST
    """
    rows = _query(sql, params)

    if not rows:
        return {
            "tournament": tournament,
            "market_type_filter": market_type,
            "count": 0,
            "market_groups": {},
        }

    # Remap market types for consolidated orphan data
    if is_pga:
        for row in rows:
            row["market_type"] = _remap_market_type(
                row.get("source_tournament", ""),
                row.get("market_type", "other"),
                row.get("market_name", ""),
            )

    # Group rows by market_type → market_name
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        mt = row.get("market_type") or "other"
        mn = row.get("market_name") or mt
        grouped[mt][mn].append(_serialise(row))

    # Build structured response
    market_groups: Dict[str, Any] = {}
    for mt, markets in grouped.items():
        market_list = []
        for mn, selections in markets.items():
            market_list.append({
                "market_name": mn,
                "selection_count": len(selections),
                "selections": selections,
            })
        market_groups[mt] = {
            "market_type": mt,
            "market_count": len(market_list),
            "total_selections": sum(m["selection_count"] for m in market_list),
            "markets": market_list,
        }

    return {
        "tournament": tournament,
        "market_type_filter": market_type,
        "count": len(rows),
        "market_groups": market_groups,
    }
