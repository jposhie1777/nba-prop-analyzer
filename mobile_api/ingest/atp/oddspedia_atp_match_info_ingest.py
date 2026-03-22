#mobile_api/ingest/atp/oddspedia_atp_match_info_ingest.py
"""Oddspedia ATP match-info/stats ingest into BigQuery.

ATP version made parallel to EPL structure:

- weather
- keys
- betting stats
- per-match stats
- head-to-head summary + matches
- last matches
- standings
- lineups
- upcoming matches

Uses dynamic tournament config so it automatically targets the active
ATP tournament without hardcoded slug/season_id.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from oddspedia_client import OddspediaClient  # noqa: E402
from mobile_api.ingest.atp.oddspedia_tour_filter import filter_atp_matches  # noqa: E402
from mobile_api.ingest.atp.oddspedia_atp_config import get_atp_config  # noqa: E402

DATASET = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")

WEATHER_TABLE = "atp_match_weather"
KEYS_TABLE = "atp_match_keys"
BETTING_STATS_TABLE = "atp_betting_stats"
PER_MATCH_TABLE = "atp_per_match_stats"
H2H_SUMMARY_TABLE = "atp_h2h_summary"
H2H_MATCHES_TABLE = "atp_h2h_matches"
LAST_MATCHES_TABLE = "atp_last_matches"
STANDINGS_TABLE = "atp_standings"
LINEUPS_TABLE = "atp_lineups"
UPCOMING_MATCHES_TABLE = "atp_upcoming_matches"

_MATCH_BASE = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("match_key", "INT64"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("round_name", "STRING"),
]

WEATHER_SCHEMA = _MATCH_BASE + [
    bigquery.SchemaField("weather_icon", "STRING"),
    bigquery.SchemaField("weather_temp_c", "FLOAT64"),
    bigquery.SchemaField("surface", "STRING"),
    bigquery.SchemaField("prize_money", "STRING"),
    bigquery.SchemaField("prize_currency", "STRING"),
    bigquery.SchemaField("ht_rank", "STRING"),
    bigquery.SchemaField("at_rank", "STRING"),
]

KEYS_SCHEMA = _MATCH_BASE + [
    bigquery.SchemaField("rank", "INT64"),
    bigquery.SchemaField("statement", "STRING"),
    bigquery.SchemaField("teams_json", "JSON"),
]

BETTING_STATS_SCHEMA = _MATCH_BASE + [
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("sub_tab", "STRING"),
    bigquery.SchemaField("label", "STRING"),
    bigquery.SchemaField("value", "STRING"),
    bigquery.SchemaField("home", "FLOAT64"),
    bigquery.SchemaField("away", "FLOAT64"),
    bigquery.SchemaField("total_matches_home", "INT64"),
    bigquery.SchemaField("total_matches_away", "INT64"),
]

PER_MATCH_SCHEMA = _MATCH_BASE + [
    bigquery.SchemaField("tab", "STRING"),
    bigquery.SchemaField("label", "STRING"),
    bigquery.SchemaField("home", "FLOAT64"),
    bigquery.SchemaField("away", "FLOAT64"),
    bigquery.SchemaField("total_matches_home", "INT64"),
    bigquery.SchemaField("total_matches_away", "INT64"),
]

H2H_SUMMARY_SCHEMA = _MATCH_BASE + [
    bigquery.SchemaField("ht_wins", "INT64"),
    bigquery.SchemaField("at_wins", "INT64"),
    bigquery.SchemaField("draws", "INT64"),
    bigquery.SchemaField("played_matches", "INT64"),
    bigquery.SchemaField("period_years", "STRING"),
]

H2H_MATCHES_SCHEMA = _MATCH_BASE + [
    bigquery.SchemaField("h2h_match_id", "INT64"),
    bigquery.SchemaField("h2h_starttime", "TIMESTAMP"),
    bigquery.SchemaField("h2h_ht", "STRING"),
    bigquery.SchemaField("h2h_ht_id", "INT64"),
    bigquery.SchemaField("h2h_at", "STRING"),
    bigquery.SchemaField("h2h_at_id", "INT64"),
    bigquery.SchemaField("h2h_hscore", "INT64"),
    bigquery.SchemaField("h2h_ascore", "INT64"),
    bigquery.SchemaField("h2h_winner", "STRING"),
    bigquery.SchemaField("h2h_league_name", "STRING"),
    bigquery.SchemaField("h2h_league_slug", "STRING"),
    bigquery.SchemaField("h2h_is_archived", "BOOL"),
    bigquery.SchemaField("h2h_periods_json", "JSON"),
]

LAST_MATCHES_SCHEMA = _MATCH_BASE + [
    bigquery.SchemaField("side", "STRING"),
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("last_match_id", "INT64"),
    bigquery.SchemaField("last_starttime", "TIMESTAMP"),
    bigquery.SchemaField("last_ht", "STRING"),
    bigquery.SchemaField("last_at", "STRING"),
    bigquery.SchemaField("last_hscore", "INT64"),
    bigquery.SchemaField("last_ascore", "INT64"),
    bigquery.SchemaField("last_outcome", "STRING"),
    bigquery.SchemaField("last_home", "BOOL"),
    bigquery.SchemaField("last_status_json", "JSON"),
    bigquery.SchemaField("last_league_id", "INT64"),
]

STANDINGS_SCHEMA = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("match_key", "INT64"),
    bigquery.SchemaField("group_name", "STRING"),
    bigquery.SchemaField("rank", "INT64"),
    bigquery.SchemaField("team", "STRING"),
    bigquery.SchemaField("team_id", "INT64"),
    bigquery.SchemaField("played", "INT64"),
    bigquery.SchemaField("wins", "INT64"),
    bigquery.SchemaField("draws", "INT64"),
    bigquery.SchemaField("losses", "INT64"),
    bigquery.SchemaField("goals_for", "INT64"),
    bigquery.SchemaField("goals_against", "INT64"),
    bigquery.SchemaField("goal_diff", "INT64"),
    bigquery.SchemaField("points", "INT64"),
]

LINEUPS_SCHEMA = _MATCH_BASE + [
    bigquery.SchemaField("team_side", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("shirt_number", "INT64"),
    bigquery.SchemaField("position", "STRING"),
    bigquery.SchemaField("meta_json", "JSON"),
]

UPCOMING_MATCHES_SCHEMA = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("tournament_name", "STRING"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("matchup", "STRING"),
    bigquery.SchemaField("start_time_utc", "TIMESTAMP"),
]


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None and v != "" else None
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None and v != "" else None
    except Exception:
        return None


def _parse_start_time_utc(value: Any) -> Optional[datetime]:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    candidate = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_start_time_utc(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_upcoming_match_rows(
    matches: List[Dict[str, Any]],
    *,
    ingested_at: str,
    scraped_date: str,
    now_utc: datetime,
    tournament_name: str,
) -> List[Dict[str, Any]]:
    by_match_id: Dict[int, tuple] = {}

    for match in matches:
        match_id = _safe_int(match.get("match_id"))
        if match_id is None:
            continue

        info = match.get("match_info") or {}
        home_team = (match.get("home_team") or info.get("ht") or "").strip()
        away_team = (match.get("away_team") or info.get("at") or "").strip()
        if not home_team or not away_team:
            continue

        start_raw = match.get("date_utc") or info.get("starttime")
        start_dt = _parse_start_time_utc(start_raw)
        if start_dt is None or start_dt < now_utc:
            continue

        row = {
            "ingested_at": ingested_at,
            "scraped_date": scraped_date,
            "match_id": match_id,
            "tournament_name": tournament_name,
            "home_team": home_team,
            "away_team": away_team,
            "matchup": f"{home_team} vs {away_team}",
            "start_time_utc": _format_start_time_utc(start_dt),
        }
        existing = by_match_id.get(match_id)
        if existing is None or start_dt < existing[0]:
            by_match_id[match_id] = (start_dt, row)

    return [
        record
        for _, record in sorted(
            by_match_id.values(),
            key=lambda item: (item[0], item[1]["matchup"]),
        )
    ]


def _base_row(
    match_id: int,
    match_key: Optional[int],
    data: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> Dict[str, Any]:
    return {
        "ingested_at": ingested_at,
        "scraped_date": scraped_date,
        "match_id": match_id,
        "match_key": match_key,
        "home_team": data.get("ht"),
        "away_team": data.get("at"),
        "date_utc": (data.get("starttime") or "").split("+")[0].strip() or None,
        "round_name": data.get("round_name"),
    }


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


def _ensure_table(
    client: bigquery.Client, table: str, schema: List[bigquery.SchemaField]
) -> None:
    table_id = _full_table_id(client, table)
    try:
        client.get_table(table_id)
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=schema))


def _truncate_and_insert(
    client: bigquery.Client, table: str, rows: List[Dict[str, Any]]
) -> int:
    table_id = _full_table_id(client, table)
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    if not rows:
        return 0
    for i in range(0, len(rows), 1000):
        errors = client.insert_rows_json(table_id, rows[i:i + 1000])
        if errors:
            raise RuntimeError(f"BigQuery insert errors for {table}: {errors}")
        time.sleep(0.2)
    return len(rows)


def _build_betting_rows(
    match_id: int,
    mk: Optional[int],
    data: Dict[str, Any],
    betting_stats: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    base = _base_row(match_id, mk, data, ingested_at, scraped_date)

    # ATP flat style
    if isinstance(betting_stats.get("data"), dict):
        block = betting_stats.get("data") or {}
        tm = block.get("total_matches") or {}
        for stat in (block.get("data") or []):
            if not isinstance(stat, dict):
                continue
            rows.append({
                **base,
                "category": block.get("label") or "total",
                "sub_tab": None,
                "label": stat.get("label"),
                "value": str(stat.get("value")) if stat.get("value") is not None else None,
                "home": _safe_float(stat.get("home")),
                "away": _safe_float(stat.get("away")),
                "total_matches_home": _safe_int(tm.get("home")),
                "total_matches_away": _safe_int(tm.get("away")),
            })
        return rows

    # Nested style fallback (EPL/MLS)
    for category in (betting_stats.get("data") or []):
        for sub_tab in (category.get("data") or []):
            tm = sub_tab.get("total_matches") or {}
            for stat in (sub_tab.get("data") or []):
                if not isinstance(stat, dict):
                    continue
                rows.append({
                    **base,
                    "category": category.get("label"),
                    "sub_tab": sub_tab.get("label"),
                    "label": stat.get("label"),
                    "value": str(stat.get("value")) if stat.get("value") is not None else None,
                    "home": _safe_float(stat.get("home")),
                    "away": _safe_float(stat.get("away")),
                    "total_matches_home": _safe_int(tm.get("home")),
                    "total_matches_away": _safe_int(tm.get("away")),
                })
    return rows


def _build_per_match_rows(
    match_id: int,
    mk: Optional[int],
    data: Dict[str, Any],
    per_match: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    base = _base_row(match_id, mk, data, ingested_at, scraped_date)

    for tab in (per_match.get("tabs") or per_match.get("data") or []):
        if not isinstance(tab, dict):
            continue
        tab_name = tab.get("label")
        tm = tab.get("total_matches") or {}
        for stat in (tab.get("data") or []):
            if not isinstance(stat, dict):
                continue
            rows.append({
                **base,
                "tab": tab_name,
                "label": stat.get("label"),
                "home": _safe_float(stat.get("home")),
                "away": _safe_float(stat.get("away")),
                "total_matches_home": _safe_int(tm.get("home")),
                "total_matches_away": _safe_int(tm.get("away")),
            })
    return rows


def ingest_match_info(
    url: Optional[str] = None,
    *,
    dry_run: bool = False,
    today_only: bool = True,
) -> Dict[str, Any]:
    # Resolve tournament config
    cfg = get_atp_config()
    target_url = url or cfg.url
    league_slug = cfg.league_slug
    season_id = cfg.season_id

    now = datetime.now(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    print(f"[atp_match_info] Tournament : {cfg.tournament_name}")
    print(f"[atp_match_info] URL        : {target_url}")
    print(f"[atp_match_info] Slug       : {league_slug}")
    print(f"[atp_match_info] Season ID  : {season_id}")

    scraper = OddspediaClient()
    matches = scraper.scrape(
        target_url,
        league_category="usa",
        league_slug=league_slug,
        season_id=season_id,
        sport="tennis",
    )
    print(f"[atp_match_info] Scraped {len(matches)} matches before tour filter")

    matches = filter_atp_matches(
        matches,
        target_league_slug=league_slug,
        target_season_id=season_id,
        log_prefix="[atp_match_info]",
    )

    upcoming_match_rows = _build_upcoming_match_rows(
        matches,
        ingested_at=ingested_at,
        scraped_date=scraped_date,
        now_utc=now,
        tournament_name=cfg.tournament_name,
    )

    if today_only:
        matches = [
            m for m in matches if (m.get("date_utc") or "").startswith(scraped_date)
        ]
        print(f"[atp_match_info] After today filter: {len(matches)} matches")

    weather_rows: List[Dict[str, Any]] = []
    key_rows: List[Dict[str, Any]] = []
    betting_rows: List[Dict[str, Any]] = []
    per_match_rows: List[Dict[str, Any]] = []
    h2h_summary_rows: List[Dict[str, Any]] = []
    h2h_match_rows: List[Dict[str, Any]] = []
    last_match_rows: List[Dict[str, Any]] = []
    standings_rows: List[Dict[str, Any]] = []
    lineup_rows: List[Dict[str, Any]] = []

    for match in matches:
        mid = _safe_int(match.get("match_id"))
        data = match.get("match_info") or {}
        if not mid or not data:
            continue
        mk = _safe_int(data.get("match_key"))
        base = _base_row(mid, mk, data, ingested_at, scraped_date)

        # Weather / surface / rankings
        weather = data.get("weather") or {}
        weather_rows.append({
            **base,
            "weather_icon": weather.get("icon"),
            "weather_temp_c": _safe_float(weather.get("temperature")),
            "surface": data.get("surface"),
            "prize_money": data.get("prize_money"),
            "prize_currency": data.get("prize_currency"),
            "ht_rank": data.get("ht_rank"),
            "at_rank": data.get("at_rank"),
        })

        # Match keys
        for rank, key in enumerate(data.get("match_keys") or [], start=1):
            if not isinstance(key, dict):
                continue
            key_rows.append({
                **base,
                "rank": rank,
                "statement": key.get("statement"),
                "teams_json": json.dumps(key.get("teams") or []),
            })

        # Betting stats
        bs = match.get("betting_stats") or {}
        if bs:
            betting_rows.extend(
                _build_betting_rows(mid, mk, data, bs, ingested_at, scraped_date)
            )

        # Per-match stats
        pms = match.get("per_match_stats") or {}
        if pms:
            per_match_rows.extend(
                _build_per_match_rows(mid, mk, data, pms, ingested_at, scraped_date)
            )

        # H2H
        h2h = match.get("head_to_head") or {}
        if h2h:
            h2h_summary_rows.append({
                **base,
                "ht_wins": _safe_int(h2h.get("ht_wins")),
                "at_wins": _safe_int(h2h.get("at_wins")),
                "draws": _safe_int(h2h.get("draws")),
                "played_matches": _safe_int(h2h.get("played_matches")),
                "period_years": h2h.get("period"),
            })
            for hm in (h2h.get("matches") or []):
                if not isinstance(hm, dict):
                    continue
                h2h_match_rows.append({
                    **base,
                    "h2h_match_id": _safe_int(hm.get("id")),
                    "h2h_starttime": (hm.get("starttime") or "").split("+")[0].strip() or None,
                    "h2h_ht": hm.get("ht"),
                    "h2h_ht_id": _safe_int(hm.get("ht_id")),
                    "h2h_at": hm.get("at"),
                    "h2h_at_id": _safe_int(hm.get("at_id")),
                    "h2h_hscore": _safe_int(hm.get("hscore")),
                    "h2h_ascore": _safe_int(hm.get("ascore")),
                    "h2h_winner": hm.get("winner"),
                    "h2h_league_name": hm.get("league_name"),
                    "h2h_league_slug": hm.get("league_slug"),
                    "h2h_is_archived": bool(hm.get("is_match_archived", False)),
                    "h2h_periods_json": json.dumps(hm.get("periods") or []),
                })

        # Last matches (home/away = player perspective)
        for side_key, side_name in (
            ("last_matches_home", "home"),
            ("last_matches_away", "away"),
        ):
            lm = match.get(side_key) or {}
            if not isinstance(lm, dict):
                continue
            for player_id, block in lm.items():
                if not isinstance(block, dict):
                    continue
                for m in (block.get("matches") or []):
                    if not isinstance(m, dict):
                        continue
                    last_match_rows.append({
                        **base,
                        "side": side_name,
                        "player_id": _safe_int(player_id),
                        "last_match_id": _safe_int(m.get("id")),
                        "last_starttime": (m.get("starttime") or "").split("+")[0].strip() or None,
                        "last_ht": m.get("ht"),
                        "last_at": m.get("at"),
                        "last_hscore": _safe_int(m.get("hscore")),
                        "last_ascore": _safe_int(m.get("ascore")),
                        "last_outcome": m.get("outcome"),
                        "last_home": bool(m.get("home")) if m.get("home") is not None else None,
                        "last_status_json": json.dumps(m.get("status") or {}),
                        "last_league_id": _safe_int(m.get("league_id")),
                    })

        # Standings
        sd = match.get("standings_data") or {}
        groups = sd.get("groups") if isinstance(sd, dict) else None
        for g in (groups or []):
            group_name = g.get("name") if isinstance(g, dict) else None
            for team in (g.get("table") if isinstance(g, dict) else []) or []:
                if not isinstance(team, dict):
                    continue
                standings_rows.append({
                    "ingested_at": ingested_at,
                    "scraped_date": scraped_date,
                    "match_id": mid,
                    "match_key": mk,
                    "group_name": group_name,
                    "rank": _safe_int(team.get("rank")),
                    "team": team.get("team") or team.get("name"),
                    "team_id": _safe_int(team.get("team_id") or team.get("id")),
                    "played": _safe_int(team.get("played")),
                    "wins": _safe_int(team.get("wins")),
                    "draws": _safe_int(team.get("draws")),
                    "losses": _safe_int(team.get("losses")),
                    "goals_for": _safe_int(team.get("goals_for") or team.get("gf")),
                    "goals_against": _safe_int(
                        team.get("goals_against") or team.get("ga")
                    ),
                    "goal_diff": _safe_int(team.get("goal_diff") or team.get("gd")),
                    "points": _safe_int(team.get("points") or team.get("pts")),
                })

        # Lineups (players / coaches)
        lu = match.get("lineups") or {}
        for side in ("home", "away"):
            block = lu.get(side) if isinstance(lu, dict) else None
            if not isinstance(block, dict):
                continue
            for status in ("starting", "substitutes", "missing", "coaches"):
                for p in (block.get(status) or []):
                    if not isinstance(p, dict):
                        continue
                    lineup_rows.append({
                        **base,
                        "team_side": side,
                        "status": status,
                        "player_id": _safe_int(p.get("id") or p.get("player_id")),
                        "player_name": p.get("name"),
                        "shirt_number": _safe_int(
                            p.get("shirt_number") or p.get("number")
                        ),
                        "position": p.get("position") or p.get("pos"),
                        "meta_json": json.dumps(p),
                    })

    counts = {
        "tournament": cfg.tournament_name,
        "weather_rows": len(weather_rows),
        "key_rows": len(key_rows),
        "betting_rows": len(betting_rows),
        "per_match_rows": len(per_match_rows),
        "h2h_summary_rows": len(h2h_summary_rows),
        "h2h_match_rows": len(h2h_match_rows),
        "last_match_rows": len(last_match_rows),
        "standings_rows": len(standings_rows),
        "lineup_rows": len(lineup_rows),
        "upcoming_match_rows": len(upcoming_match_rows),
    }

    if dry_run:
        print(json.dumps(counts, indent=2))
        return {**counts, "dry_run": True}

    bq = _bq_client()
    _ensure_dataset(bq)
    table_defs = [
        (WEATHER_TABLE, WEATHER_SCHEMA, weather_rows),
        (KEYS_TABLE, KEYS_SCHEMA, key_rows),
        (BETTING_STATS_TABLE, BETTING_STATS_SCHEMA, betting_rows),
        (PER_MATCH_TABLE, PER_MATCH_SCHEMA, per_match_rows),
        (H2H_SUMMARY_TABLE, H2H_SUMMARY_SCHEMA, h2h_summary_rows),
        (H2H_MATCHES_TABLE, H2H_MATCHES_SCHEMA, h2h_match_rows),
        (LAST_MATCHES_TABLE, LAST_MATCHES_SCHEMA, last_match_rows),
        (STANDINGS_TABLE, STANDINGS_SCHEMA, standings_rows),
        (LINEUPS_TABLE, LINEUPS_SCHEMA, lineup_rows),
        (UPCOMING_MATCHES_TABLE, UPCOMING_MATCHES_SCHEMA, upcoming_match_rows),
    ]
    written: Dict[str, int] = {}
    for table, schema, rows in table_defs:
        _ensure_table(bq, table, schema)
        written[table] = _truncate_and_insert(bq, table, rows)

    return {**counts, "written": written, "errors": []}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None, help="Override tournament URL")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--all-dates", action="store_true")
    args = parser.parse_args()

    result = ingest_match_info(
        args.url, dry_run=args.dry_run, today_only=not args.all_dates
    )
    print(json.dumps(result, indent=2, default=str))
