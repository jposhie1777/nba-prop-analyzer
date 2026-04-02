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

# ── Orphan tournament consolidation ──────────────────────────────────────────
# FanDuel lists sub-markets (3 Balls, Make/Miss Cut, etc.) as separate
# "tournament" entries rather than embedding them in the main PGA weekly event.
# We consolidate them back at query time.

_ORPHAN_NAMES = frozenset({
    "3 Balls", "Make/Miss Cut", "Player Round Scores", "Specials", "Top Region",
})


def _is_pga_tournament(name: str) -> bool:
    """Return True if *name* looks like a PGA Tour weekly event."""
    return name.startswith("PGA ") and any(c.isdigit() for c in name)


def _remap_market_type(source_tourn: str, mtype: str, mname: str) -> str:
    """Assign the correct market_type after orphan consolidation."""
    # Orphan tournament overrides
    if source_tourn == "Top Region":
        return "top_nationality"
    if source_tourn == "Specials":
        return "specials"
    if source_tourn == "Player Round Scores":
        return "player_round_score"
    # 3 Balls / Make/Miss Cut already have correct types (three_ball / make_cut)

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
    main_pga = next(
        (t["tournament_name"] for t in result if _is_pga_tournament(t.get("tournament_name", ""))),
        None,
    )
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

    sql = f"""
    WITH latest_per_tourn AS (
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
      bp.season_finish_avg
    FROM raw
    LEFT JOIN `{SKILL_STATS_TABLE}` sk
      ON LOWER(TRIM(raw.player_name)) = LOWER(TRIM(sk.player_name))
    LEFT JOIN `{RECENT_FORM_TABLE}` rf
      ON LOWER(TRIM(raw.player_name)) = LOWER(TRIM(rf.player_display_name))
    LEFT JOIN `{BETTING_PROFILE_TABLE}` bp
      ON LOWER(TRIM(raw.player_name)) = LOWER(TRIM(bp.player_display_name))
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
