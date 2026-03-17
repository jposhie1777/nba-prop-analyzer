#mobile_api/ingest/epl/oddspedia

"""
Ingests match info, stats, and H2H data from Oddspedia EPL match pages into BigQuery.

Mirrors oddspedia_mls_match_info_ingest.py with EPL-specific differences:
  - Uses matchKey (not matchId) for all per-match stat endpoints
  - getOutrights returns 404 — skipped gracefully
  - Gameweek date window (not just today)
  - Extra tables for per-match stats, H2H, last matches, standings

Tables
------
  oddspedia.epl_match_weather      — matchup + weather + team form
  oddspedia.epl_match_keys         — ranked statistical statements
  oddspedia.epl_betting_stats      — goals/btts/corners betting % stats
  oddspedia.epl_per_match_stats    — season averages (shots, possession, etc.)
  oddspedia.epl_head_to_head       — H2H historical match results
  oddspedia.epl_last_matches       — recent form for home + away teams
  oddspedia.epl_standings          — full Premier League table
  oddspedia.epl_lineups            — confirmed/projected lineups

Usage
-----
    python -m mobile_api.ingest.epl.oddspedia_epl_match_info_ingest
    python -m mobile_api.ingest.epl.oddspedia_epl_match_info_ingest --dry-run
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from oddspedia_client import OddspediaClient  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_URL      = "https://www.oddspedia.com/us/soccer/england/premier-league"
ODDSPEDIA_URL    = os.getenv("ODDSPEDIA_EPL_URL", DEFAULT_URL)
DATASET          = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")

WEATHER_TABLE        = "epl_match_weather"
KEYS_TABLE           = "epl_match_keys"
BETTING_STATS_TABLE  = "epl_betting_stats"
PER_MATCH_TABLE      = "epl_per_match_stats"
H2H_TABLE            = "epl_head_to_head"
LAST_MATCHES_TABLE   = "epl_last_matches"
STANDINGS_TABLE      = "epl_standings"
LINEUPS_TABLE        = "epl_lineups"

# EPL-specific league identifiers
EPL_LEAGUE_ID    = 627
EPL_SEASON_ID    = 130281
EPL_CATEGORY     = "england"
EPL_LEAGUE_SLUG  = "premier-league"

# ── Schemas ───────────────────────────────────────────────────────────────────

_MATCH_BASE = [
    bigquery.SchemaField("ingested_at",  "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("match_id",     "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("match_key",    "INT64"),
    bigquery.SchemaField("home_team",    "STRING"),
    bigquery.SchemaField("away_team",    "STRING"),
    bigquery.SchemaField("date_utc",     "TIMESTAMP"),
    bigquery.SchemaField("round_name",   "STRING"),
]

WEATHER_SCHEMA: List[bigquery.SchemaField] = _MATCH_BASE + [
    bigquery.SchemaField("weather_icon",   "STRING"),
    bigquery.SchemaField("weather_temp_c", "FLOAT64"),
    bigquery.SchemaField("home_form",      "STRING"),
    bigquery.SchemaField("away_form",      "STRING"),
    bigquery.SchemaField("referee_name",   "STRING"),
    bigquery.SchemaField("venue_name",     "STRING"),
    bigquery.SchemaField("venue_city",     "STRING"),
    bigquery.SchemaField("venue_capacity", "INT64"),
]

KEYS_SCHEMA: List[bigquery.SchemaField] = _MATCH_BASE + [
    bigquery.SchemaField("rank",       "INT64"),
    bigquery.SchemaField("statement",  "STRING"),
    bigquery.SchemaField("teams_json", "JSON"),
]

BETTING_STATS_SCHEMA: List[bigquery.SchemaField] = _MATCH_BASE + [
    bigquery.SchemaField("category",            "STRING"),
    bigquery.SchemaField("sub_tab",             "STRING"),
    bigquery.SchemaField("label",               "STRING"),
    bigquery.SchemaField("value",               "STRING"),
    bigquery.SchemaField("home",                "FLOAT64"),
    bigquery.SchemaField("away",                "FLOAT64"),
    bigquery.SchemaField("total_matches_home",  "INT64"),
    bigquery.SchemaField("total_matches_away",  "INT64"),
]

PER_MATCH_SCHEMA: List[bigquery.SchemaField] = _MATCH_BASE + [
    bigquery.SchemaField("tab",                 "STRING"),
    bigquery.SchemaField("label",               "STRING"),
    bigquery.SchemaField("home",                "FLOAT64"),
    bigquery.SchemaField("away",                "FLOAT64"),
    bigquery.SchemaField("total_matches_home",  "INT64"),
    bigquery.SchemaField("total_matches_away",  "INT64"),
]

H2H_SCHEMA: List[bigquery.SchemaField] = _MATCH_BASE + [
    bigquery.SchemaField("ht_wins",         "INT64"),
    bigquery.SchemaField("at_wins",         "INT64"),
    bigquery.SchemaField("draws",           "INT64"),
    bigquery.SchemaField("played_matches",  "INT64"),
    bigquery.SchemaField("h2h_match_id",    "INT64"),
    bigquery.SchemaField("h2h_starttime",   "TIMESTAMP"),
    bigquery.SchemaField("h2h_ht",          "STRING"),
    bigquery.SchemaField("h2h_at",          "STRING"),
    bigquery.SchemaField("h2h_hscore",      "INT64"),
    bigquery.SchemaField("h2h_ascore",      "INT64"),
    bigquery.SchemaField("h2h_winner",      "INT64"),   # 0=draw,1=home,2=away
    bigquery.SchemaField("h2h_league",      "STRING"),
    bigquery.SchemaField("h2h_periods",     "JSON"),
]

LAST_MATCHES_SCHEMA: List[bigquery.SchemaField] = _MATCH_BASE + [
    bigquery.SchemaField("side",            "STRING"),  # "home" or "away"
    bigquery.SchemaField("lm_match_id",     "INT64"),
    bigquery.SchemaField("lm_date",         "TIMESTAMP"),
    bigquery.SchemaField("lm_ht",           "STRING"),
    bigquery.SchemaField("lm_at",           "STRING"),
    bigquery.SchemaField("lm_ht_id",        "INT64"),
    bigquery.SchemaField("lm_at_id",        "INT64"),
    bigquery.SchemaField("lm_hscore",       "INT64"),
    bigquery.SchemaField("lm_ascore",       "INT64"),
    bigquery.SchemaField("lm_outcome",      "STRING"),  # w/d/l
    bigquery.SchemaField("lm_home",         "BOOL"),    # was this team playing at home?
    bigquery.SchemaField("lm_league_id",    "INT64"),
    bigquery.SchemaField("lm_matchstatus",  "INT64"),
    bigquery.SchemaField("lm_match_key",    "INT64"),
    bigquery.SchemaField("lm_periods",      "JSON"),
]

STANDINGS_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at",             "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date",            "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("match_id",                "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("match_key",               "INT64"),
    bigquery.SchemaField("generated_at",            "TIMESTAMP"),
    bigquery.SchemaField("rank",                    "INT64"),
    bigquery.SchemaField("rank_movement",           "INT64"),
    bigquery.SchemaField("team_id",                 "INT64"),
    bigquery.SchemaField("team_name",               "STRING"),
    bigquery.SchemaField("team_abbr",               "STRING"),
    bigquery.SchemaField("team_slug",               "STRING"),
    bigquery.SchemaField("current_outcome",         "STRING"),
    bigquery.SchemaField("current_outcome_slug",    "STRING"),
    bigquery.SchemaField("played",                  "INT64"),
    bigquery.SchemaField("win",                     "INT64"),
    bigquery.SchemaField("draw",                    "INT64"),
    bigquery.SchemaField("loss",                    "INT64"),
    bigquery.SchemaField("goals_for",               "INT64"),
    bigquery.SchemaField("goals_against",           "INT64"),
    bigquery.SchemaField("goal_diff",               "INT64"),
    bigquery.SchemaField("points",                  "INT64"),
    bigquery.SchemaField("form",                    "STRING"),
    bigquery.SchemaField("win_ratio",               "FLOAT64"),
    bigquery.SchemaField("last_10",                 "STRING"),
]

LINEUPS_SCHEMA: List[bigquery.SchemaField] = _MATCH_BASE + [
    bigquery.SchemaField("side",                "STRING"),  # home/away
    bigquery.SchemaField("formation",           "STRING"),
    bigquery.SchemaField("manager",             "STRING"),
    bigquery.SchemaField("confirmed",           "BOOL"),
    bigquery.SchemaField("player_key",          "INT64"),
    bigquery.SchemaField("player_name",         "STRING"),
    bigquery.SchemaField("player_number",       "INT64"),
    bigquery.SchemaField("player_position",     "STRING"),
    bigquery.SchemaField("player_substituted",  "INT64"),
    bigquery.SchemaField("is_bench",            "BOOL"),
]

# ── BigQuery helpers ──────────────────────────────────────────────────────────


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _full_table_id(client: bigquery.Client, table: str) -> str:
    return f"{client.project}.{DATASET}.{table}"


def _ensure_dataset(client: bigquery.Client) -> None:
    dataset_ref = f"{client.project}.{DATASET}"
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = DATASET_LOCATION
        client.create_dataset(ds)
        print(f"[epl_info] Created dataset {DATASET}")
    except Conflict:
        pass


def _ensure_table(
    client: bigquery.Client,
    table: str,
    schema: List[bigquery.SchemaField],
) -> None:
    table_id = _full_table_id(client, table)
    try:
        client.get_table(table_id)
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=schema))
        print(f"[epl_info] Created table {DATASET}.{table}")
    except Conflict:
        pass


def _truncate_and_insert(
    client: bigquery.Client,
    table: str,
    rows: List[Dict[str, Any]],
    chunk_size: int = 500,
) -> int:
    if not rows:
        return 0
    table_id = _full_table_id(client, table)
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    written = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i: i + chunk_size]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
        written += len(chunk)
        time.sleep(0.05)
    return written


# ── Shared helpers ────────────────────────────────────────────────────────────


def _ts(value: Optional[str]) -> Optional[str]:
    """Strip timezone offset for BigQuery TIMESTAMP."""
    if not value:
        return None
    return value.split("+")[0].strip()


def _safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _match_base(
    match_id: int,
    match_key: Optional[int],
    data: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> Dict[str, Any]:
    return {
        "ingested_at":  ingested_at,
        "scraped_date": scraped_date,
        "match_id":     match_id,
        "match_key":    match_key,
        "home_team":    data.get("ht"),
        "away_team":    data.get("at"),
        "date_utc":     _ts(data.get("starttime") or data.get("md")),
        "round_name":   data.get("round_name"),
    }


# ── Row builders ──────────────────────────────────────────────────────────────


def _build_weather_row(
    mid: int, mk: Optional[int], data: Dict[str, Any],
    ingested_at: str, scraped_date: str,
) -> Dict[str, Any]:
    weather = data.get("weather_conditions") or {}
    return {
        **_match_base(mid, mk, data, ingested_at, scraped_date),
        "weather_icon":   weather.get("icon"),
        "weather_temp_c": weather.get("temperature"),
        "home_form":      data.get("ht_form"),
        "away_form":      data.get("at_form"),
        "referee_name":   data.get("referee_name"),
        "venue_name":     data.get("venue_name"),
        "venue_city":     data.get("venue_city"),
        "venue_capacity": _safe_int(data.get("venue_capacity")),
    }


def _build_key_rows(
    mid: int, mk: Optional[int], data: Dict[str, Any],
    ingested_at: str, scraped_date: str,
) -> List[Dict[str, Any]]:
    base = _match_base(mid, mk, data, ingested_at, scraped_date)
    rows = []
    for rank, key in enumerate(data.get("match_keys") or [], start=1):
        if not isinstance(key, dict):
            continue
        rows.append({
            **base,
            "rank":       rank,
            "statement":  key.get("statement"),
            "teams_json": json.dumps(key.get("teams") or []),
        })
    return rows


def _build_betting_stats_rows(
    mid: int, mk: Optional[int], data: Dict[str, Any],
    betting_stats: Dict[str, Any],
    ingested_at: str, scraped_date: str,
) -> List[Dict[str, Any]]:
    base = _match_base(mid, mk, data, ingested_at, scraped_date)
    rows = []

    for category in (betting_stats.get("data") or []):
        cat_label = category.get("label")
        for sub_tab in (category.get("data") or []):
            sub_label     = sub_tab.get("label")
            total_matches = sub_tab.get("total_matches") or {}
            for stat in (sub_tab.get("data") or []):
                if not isinstance(stat, dict):
                    continue
                if stat.get("label") == "scoring_minutes":
                    continue  # nested structure — skip for flat table
                rows.append({
                    **base,
                    "category":           cat_label,
                    "sub_tab":            sub_label,
                    "label":              stat.get("label"),
                    "value":              str(stat["value"]) if stat.get("value") is not None else None,
                    "home":               float(stat["home"]) if stat.get("home") is not None else None,
                    "away":              float(stat["away"]) if stat.get("away") is not None else None,
                    "total_matches_home": total_matches.get("home"),
                    "total_matches_away": total_matches.get("away"),
                })
    return rows


def _build_per_match_stat_rows(
    mid: int, mk: Optional[int], data: Dict[str, Any],
    per_match_stats: Dict[str, Any],
    ingested_at: str, scraped_date: str,
) -> List[Dict[str, Any]]:
    base = _match_base(mid, mk, data, ingested_at, scraped_date)
    total = per_match_stats.get("total_matches") or {}
    rows = []

    for tab in (per_match_stats.get("data") or []):
        tab_label = tab.get("label")
        for stat in (tab.get("data") or []):
            if not isinstance(stat, dict):
                continue
            rows.append({
                **base,
                "tab":                tab_label,
                "label":              stat.get("label"),
                "home":               float(stat["home"]) if stat.get("home") is not None else None,
                "away":               float(stat["away"]) if stat.get("away") is not None else None,
                "total_matches_home": _safe_int(total.get("home")),
                "total_matches_away": _safe_int(total.get("away")),
            })
    return rows


def _build_h2h_rows(
    mid: int, mk: Optional[int], data: Dict[str, Any],
    h2h: Dict[str, Any],
    ingested_at: str, scraped_date: str,
) -> List[Dict[str, Any]]:
    base = _match_base(mid, mk, data, ingested_at, scraped_date)
    rows = []

    summary = {
        "ht_wins":        h2h.get("ht_wins"),
        "at_wins":        h2h.get("at_wins"),
        "draws":          h2h.get("draws"),
        "played_matches": h2h.get("played_matches"),
    }

    for match in (h2h.get("matches") or []):
        rows.append({
            **base,
            **summary,
            "h2h_match_id":   match.get("id"),
            "h2h_starttime":  _ts(match.get("starttime")),
            "h2h_ht":         match.get("ht"),
            "h2h_at":         match.get("at"),
            "h2h_hscore":     _safe_int(match.get("hscore")),
            "h2h_ascore":     _safe_int(match.get("ascore")),
            "h2h_winner":     _safe_int(match.get("winner")),
            "h2h_league":     match.get("league_name"),
            "h2h_periods":    match.get("periods"),  # already a JSON string from API
        })
    return rows


def _build_last_match_rows(
    mid: int, mk: Optional[int], data: Dict[str, Any],
    last_matches_home: Dict[str, Any],
    last_matches_away: Dict[str, Any],
    ingested_at: str, scraped_date: str,
) -> List[Dict[str, Any]]:
    base = _match_base(mid, mk, data, ingested_at, scraped_date)
    rows = []

    def _process(side: str, team_data: Dict[str, Any]) -> None:
        for m in (team_data.get("matches") or []):
            # Skip the upcoming match itself (no score yet)
            if m.get("matchstatus") == 1 and m.get("hscore") is None:
                continue
            rows.append({
                **base,
                "side":           side,
                "lm_match_id":    m.get("id"),
                "lm_date":        _ts(m.get("md")),
                "lm_ht":          m.get("ht"),
                "lm_at":          m.get("at"),
                "lm_ht_id":       _safe_int(m.get("ht_id")),
                "lm_at_id":       _safe_int(m.get("at_id")),
                "lm_hscore":      _safe_int(m.get("hscore")),
                "lm_ascore":      _safe_int(m.get("ascore")),
                "lm_outcome":     m.get("outcome"),
                "lm_home":        bool(m.get("home", False)),
                "lm_league_id":   _safe_int(m.get("league_id")),
                "lm_matchstatus": _safe_int(m.get("matchstatus")),
                "lm_match_key":   _safe_int(m.get("match_key")),
                "lm_periods":     m.get("status"),  # JSON string of period scores
            })

    ht_id = str(data.get("ht_id", ""))
    at_id = str(data.get("at_id", ""))

    home_data = last_matches_home.get(ht_id) or {}
    away_data = last_matches_away.get(at_id) or {}

    _process("home", home_data)
    _process("away", away_data)
    return rows


def _build_standings_rows(
    mid: int, mk: Optional[int],
    standings_data: Dict[str, Any],
    ingested_at: str, scraped_date: str,
) -> List[Dict[str, Any]]:
    rows = []
    generated_at = _ts(standings_data.get("generated_at"))

    for group_name, group in (standings_data.get("standings") or {}).items():
        # Use "total" standings; skip home/away splits
        for row in (group.get("total") or []):
            rows.append({
                "ingested_at":          ingested_at,
                "scraped_date":         scraped_date,
                "match_id":             mid,
                "match_key":            mk,
                "generated_at":         generated_at,
                "rank":                 _safe_int(row.get("rank")),
                "rank_movement":        _safe_int(row.get("rank_movement")),
                "team_id":              _safe_int(row.get("team_id")),
                "team_name":            row.get("team_name"),
                "team_abbr":            row.get("team_abbr"),
                "team_slug":            row.get("team_slug"),
                "current_outcome":      row.get("current_outcome"),
                "current_outcome_slug": row.get("current_outcome_slug"),
                "played":               _safe_int(row.get("played")),
                "win":                  _safe_int(row.get("win")),
                "draw":                 _safe_int(row.get("draw")),
                "loss":                 _safe_int(row.get("loss")),
                "goals_for":            _safe_int(row.get("goals_for")),
                "goals_against":        _safe_int(row.get("goals_against")),
                "goal_diff":            _safe_int(row.get("goal_diff")),
                "points":               _safe_int(row.get("points")),
                "form":                 row.get("form"),
                "win_ratio":            row.get("win_ratio"),
                "last_10":              row.get("last_10"),
            })
    return rows


def _build_lineup_rows(
    mid: int, mk: Optional[int], data: Dict[str, Any],
    lineups_data: Dict[str, Any],
    ingested_at: str, scraped_date: str,
) -> List[Dict[str, Any]]:
    base      = _match_base(mid, mk, data, ingested_at, scraped_date)
    confirmed = lineups_data.get("confirmed", False)
    rows      = []

    def _process_players(side: str, is_bench: bool, players: List[Dict]) -> None:
        formation = (lineups_data.get("lineups") or {}).get(side, {}).get("formation")
        manager   = (lineups_data.get("lineups") or {}).get(side, {}).get("manager")
        for p in (players or []):
            rows.append({
                **base,
                "side":               side,
                "formation":          formation,
                "manager":            manager,
                "confirmed":          confirmed,
                "player_key":         _safe_int(p.get("key")),
                "player_name":        p.get("player_name"),
                "player_number":      _safe_int(p.get("player_number")),
                "player_position":    p.get("player_position"),
                "player_substituted": _safe_int(p.get("player_substituted")),
                "is_bench":           is_bench,
            })

    for side in ("home", "away"):
        lineup = (lineups_data.get("lineups") or {}).get(side, {})
        bench  = (lineups_data.get("benches") or {}).get(side, [])
        _process_players(side, False, lineup.get("players") or [])
        _process_players(side, True,  bench)

    return rows


# ── Scraper: fetch all per-match data via Camoufox/Vuex ──────────────────────


def _fetch_epl_match_data(
    page: Any,
    match: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Navigate to an EPL match page and extract all stats from the Vuex store.

    Returns a dict with keys:
      match_info, betting_stats, per_match_stats, head_to_head,
      last_matches_home, last_matches_away, standings_data, lineups
    """
    mid      = match.get("match_id")
    mk       = match.get("match_key")
    ht_slug  = match.get("ht_slug") or ""
    at_slug  = match.get("at_slug") or ""

    if not mk and not (ht_slug and at_slug):
        print(f"[epl_info] match={mid} missing match_key and slugs — skipping stats")
        return {}

    match_url = (
        f"https://oddspedia.com/us/soccer/{EPL_CATEGORY}/{EPL_LEAGUE_SLUG}"
        f"/{ht_slug}-{at_slug}-{mk}"
    )
    print(f"[epl_info] Loading match page: {match_url}")

    try:
        page.goto(match_url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(4000)  # let Vuex hydrate all store keys
    except Exception as exc:
        print(f"[epl_info] match={mid} page load error: {exc}")
        return {}

    result = page.evaluate("""
        () => {
            try {
                const store = document.querySelector('#__nuxt')?.__vue__?.$store?.state?.event;
                if (!store) return null;
                return {
                    event:            JSON.parse(JSON.stringify(store.event           || {})),
                    bettingStats:     JSON.parse(JSON.stringify(store.bettingStats    || null)),
                    perMatchStats:    JSON.parse(JSON.stringify(store.perMatchStats   || null)),
                    headToHead:       JSON.parse(JSON.stringify(store.headToHead      || null)),
                    lastMatchesHome:  JSON.parse(JSON.stringify(store.lastMatchesHome || {})),
                    lastMatchesAway:  JSON.parse(JSON.stringify(store.lastMatchesAway || {})),
                    standingsData:    JSON.parse(JSON.stringify(store.standingsData   || null)),
                    lineups:          JSON.parse(JSON.stringify(store.lineups         || null)),
                };
            } catch(e) {
                return { error: e.toString() };
            }
        }
    """)

    if not result or result.get("error"):
        print(f"[epl_info] match={mid} Vuex extraction failed: {result}")
        return {}

    # If bettingStats not yet loaded, trigger it explicitly
    if not result.get("bettingStats"):
        print(f"[epl_info] match={mid} bettingStats empty — triggering fetch")
        try:
            page.evaluate("""
                async () => {
                    const store = document.querySelector('#__nuxt').__vue__.$store;
                    const axios = document.querySelector('#__nuxt').__vue__.$axios;
                    const cancelToken = axios.CancelToken.source().token;
                    await store.dispatch('event/fetchBettingStats', { cancelToken });
                }
            """)
            page.wait_for_timeout(2000)
            betting_stats = page.evaluate("""
                () => {
                    try {
                        const s = document.querySelector('#__nuxt')?.__vue__?.$store?.state?.event?.bettingStats;
                        return s ? JSON.parse(JSON.stringify(s)) : null;
                    } catch(e) { return null; }
                }
            """)
            result["bettingStats"] = betting_stats
        except Exception as exc:
            print(f"[epl_info] match={mid} bettingStats fetch error: {exc}")

    return result


# ── Main ingest ───────────────────────────────────────────────────────────────


def ingest_epl_match_info(
    url: Optional[str] = None,
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    target_url = url or ODDSPEDIA_URL
    now          = datetime.now(timezone.utc)
    ingested_at  = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    print("=" * 60)
    print("[epl_info] Starting Oddspedia EPL match info ingest")
    print(f"[epl_info] URL         : {target_url}")
    print(f"[epl_info] Ingested at : {ingested_at}")
    if dry_run:
        print("[epl_info] Mode        : DRY RUN")
    print("=" * 60)

    scraper = OddspediaClient()
    matches = scraper.scrape(
        target_url,
        league_category="england",
        league_slug="premier-league",
        season_id=130281,
        sport="soccer",
    )
    print(f"[epl_info] {len(matches)} matches scraped from listing page")

    # Filter out postponed
    active_matches = [
        m for m in matches
        if m.get("matchstatus") != 4 and m.get("special_status") != "Postponed"
    ]
    print(f"[epl_info] {len(active_matches)} active (non-postponed) matches")

    # ── Accumulate rows across all match pages ─────────────────────────────
    weather_rows      : List[Dict] = []
    key_rows          : List[Dict] = []
    betting_stat_rows : List[Dict] = []
    per_match_rows    : List[Dict] = []
    h2h_rows          : List[Dict] = []
    last_match_rows   : List[Dict] = []
    standings_rows    : List[Dict] = []
    lineup_rows       : List[Dict] = []

    # Re-use the same Camoufox session for all match pages
    from camoufox.sync_api import Camoufox

    with Camoufox(headless=True, geoip=True) as browser:
        context = browser.new_context(
            locale="en-US",
            viewport={"width": 1920, "height": 1080},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = context.new_page()

        # First load the listing page to establish cookies/session
        print(f"[epl_info] Loading listing page for session: {target_url}")
        page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3000)

        for match in active_matches:
            mid = match.get("match_id")
            if not mid:
                continue

            print(f"[epl_info] Processing match={mid}: {match.get('home_team')} vs {match.get('away_team')}")

            store_data = _fetch_epl_match_data(page, match)
            if not store_data:
                print(f"[epl_info] match={mid} no store data — skipping")
                continue

            # The event object from the store has the richest match info
            event_data = store_data.get("event") or match.get("match_info") or {}
            mk         = _safe_int(event_data.get("match_key") or match.get("match_key"))

            print(f"[epl_info] match={mid} match_key={mk}")

            # Weather / venue / form
            weather_rows.append(
                _build_weather_row(mid, mk, event_data, ingested_at, scraped_date)
            )

            # Match keys (betting trend statements)
            key_rows.extend(
                _build_key_rows(mid, mk, event_data, ingested_at, scraped_date)
            )

            # Betting stats
            bs = store_data.get("bettingStats")
            if bs:
                rows = _build_betting_stats_rows(mid, mk, event_data, bs, ingested_at, scraped_date)
                betting_stat_rows.extend(rows)
                print(f"[epl_info] match={mid} betting_stats rows: {len(rows)}")
            else:
                print(f"[epl_info] match={mid} no bettingStats")

            # Per-match stats (season averages)
            pms = store_data.get("perMatchStats")
            if pms:
                rows = _build_per_match_stat_rows(mid, mk, event_data, pms, ingested_at, scraped_date)
                per_match_rows.extend(rows)
                print(f"[epl_info] match={mid} per_match_stats rows: {len(rows)}")

            # Head-to-head
            h2h = store_data.get("headToHead")
            if h2h:
                rows = _build_h2h_rows(mid, mk, event_data, h2h, ingested_at, scraped_date)
                h2h_rows.extend(rows)
                print(f"[epl_info] match={mid} h2h rows: {len(rows)}")

            # Last matches (home + away recent form)
            lmh = store_data.get("lastMatchesHome") or {}
            lma = store_data.get("lastMatchesAway") or {}
            if lmh or lma:
                rows = _build_last_match_rows(mid, mk, event_data, lmh, lma, ingested_at, scraped_date)
                last_match_rows.extend(rows)
                print(f"[epl_info] match={mid} last_matches rows: {len(rows)}")

            # Standings
            sd = store_data.get("standingsData")
            if sd:
                rows = _build_standings_rows(mid, mk, sd, ingested_at, scraped_date)
                standings_rows.extend(rows)
                print(f"[epl_info] match={mid} standings rows: {len(rows)}")

            # Lineups
            lu = store_data.get("lineups")
            if lu:
                rows = _build_lineup_rows(mid, mk, event_data, lu, ingested_at, scraped_date)
                lineup_rows.extend(rows)
                print(f"[epl_info] match={mid} lineup rows: {len(rows)}")

    # ── Summary ───────────────────────────────────────────────────────────
    counts = {
        "weather_rows":       len(weather_rows),
        "key_rows":           len(key_rows),
        "betting_stat_rows":  len(betting_stat_rows),
        "per_match_rows":     len(per_match_rows),
        "h2h_rows":           len(h2h_rows),
        "last_match_rows":    len(last_match_rows),
        "standings_rows":     len(standings_rows),
        "lineup_rows":        len(lineup_rows),
    }
    for name, count in counts.items():
        print(f"[epl_info] {name:<25}: {count}")

    if dry_run:
        print("\n--- WEATHER SAMPLE ---")
        print(json.dumps(weather_rows[:2],      indent=2, default=str))
        print("\n--- KEYS SAMPLE ---")
        print(json.dumps(key_rows[:3],          indent=2, default=str))
        print("\n--- BETTING STATS SAMPLE ---")
        print(json.dumps(betting_stat_rows[:5], indent=2, default=str))
        print("\n--- H2H SAMPLE ---")
        print(json.dumps(h2h_rows[:3],          indent=2, default=str))
        print("\n--- LAST MATCHES SAMPLE ---")
        print(json.dumps(last_match_rows[:5],   indent=2, default=str))
        print("\n--- STANDINGS SAMPLE ---")
        print(json.dumps(standings_rows[:3],    indent=2, default=str))
        print("\n--- LINEUPS SAMPLE ---")
        print(json.dumps(lineup_rows[:5],       indent=2, default=str))
        return {**counts, "dry_run": True}

    bq = _bq_client()
    _ensure_dataset(bq)

    all_tables = [
        (WEATHER_TABLE,       WEATHER_SCHEMA,       weather_rows),
        (KEYS_TABLE,          KEYS_SCHEMA,           key_rows),
        (BETTING_STATS_TABLE, BETTING_STATS_SCHEMA,  betting_stat_rows),
        (PER_MATCH_TABLE,     PER_MATCH_SCHEMA,      per_match_rows),
        (H2H_TABLE,           H2H_SCHEMA,            h2h_rows),
        (LAST_MATCHES_TABLE,  LAST_MATCHES_SCHEMA,   last_match_rows),
        (STANDINGS_TABLE,     STANDINGS_SCHEMA,      standings_rows),
        (LINEUPS_TABLE,       LINEUPS_SCHEMA,        lineup_rows),
    ]

    written: Dict[str, int] = {}
    for table_name, schema, rows in all_tables:
        _ensure_table(bq, table_name, schema)
        n = _truncate_and_insert(bq, table_name, rows)
        written[table_name] = n
        print(f"[epl_info] {table_name}: {n} rows written")

    print("=" * 60)
    return {**counts, "written": written, "errors": []}


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Oddspedia EPL match info/stats and load into BigQuery."
    )
    parser.add_argument("--url",      default=None)
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    result = ingest_epl_match_info(url=args.url, dry_run=args.dry_run)
    print(json.dumps(result, indent=2, default=str))