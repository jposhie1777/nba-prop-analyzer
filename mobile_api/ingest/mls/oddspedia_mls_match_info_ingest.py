# mobile_api/ingest/mls/oddspedia_mls_match_info_ingest.py
"""
Ingests match info and match keys from Oddspedia getMatchInfo API into BigQuery.

Tables:
  oddspedia.mls_match_weather  — matchup + weather + team form
  oddspedia.mls_match_keys     — matchup + ranked statistical statements
  oddspedia.mls_betting_stats  — matchup + goals/btts/corners/cards betting stats
  oddspedia.mls_last_matches   — recent form for home + away teams
  oddspedia.mls_upcoming_matches — upcoming match schedule

Usage:
    python -m mobile_api.ingest.mls.oddspedia_mls_match_info_ingest
    python -m mobile_api.ingest.mls.oddspedia_mls_match_info_ingest --dry-run
    python -m mobile_api.ingest.mls.oddspedia_mls_match_info_ingest --scrape-only
    python -m mobile_api.ingest.mls.oddspedia_mls_match_info_ingest --load-only
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

DEFAULT_URL = "https://www.oddspedia.com/us/soccer/usa/mls"
ODDSPEDIA_URL = os.getenv("ODDSPEDIA_MLS_URL", DEFAULT_URL)
DATASET = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")
WEATHER_TABLE = "mls_match_weather"
KEYS_TABLE = "mls_match_keys"
BETTING_STATS_TABLE = "mls_betting_stats"
LAST_MATCHES_TABLE = "mls_last_matches"
UPCOMING_MATCHES_TABLE = "mls_upcoming_matches"

# ── Schemas ───────────────────────────────────────────────────────────────────

WEATHER_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("weather_icon", "STRING"),
    bigquery.SchemaField("weather_temp_c", "FLOAT64"),
    bigquery.SchemaField("home_form", "STRING"),
    bigquery.SchemaField("away_form", "STRING"),
]

KEYS_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("rank", "INT64"),
    bigquery.SchemaField("statement", "STRING"),
    bigquery.SchemaField("teams_json", "JSON"),
]

BETTING_STATS_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("sub_tab", "STRING"),
    bigquery.SchemaField("label", "STRING"),
    bigquery.SchemaField("value", "STRING"),
    bigquery.SchemaField("home", "FLOAT64"),
    bigquery.SchemaField("away", "FLOAT64"),
    bigquery.SchemaField("total_matches_home", "INT64"),
    bigquery.SchemaField("total_matches_away", "INT64"),
]

LAST_MATCHES_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("side", "STRING"),       # "home" or "away"
    bigquery.SchemaField("lm_match_id", "INT64"),
    bigquery.SchemaField("lm_date", "TIMESTAMP"),
    bigquery.SchemaField("lm_ht", "STRING"),
    bigquery.SchemaField("lm_at", "STRING"),
    bigquery.SchemaField("lm_ht_id", "INT64"),
    bigquery.SchemaField("lm_at_id", "INT64"),
    bigquery.SchemaField("lm_hscore", "INT64"),
    bigquery.SchemaField("lm_ascore", "INT64"),
    bigquery.SchemaField("lm_outcome", "STRING"),
    bigquery.SchemaField("lm_home", "BOOL"),
    bigquery.SchemaField("lm_league_id", "INT64"),
    bigquery.SchemaField("lm_matchstatus", "INT64"),
    bigquery.SchemaField("lm_match_key", "INT64"),
    bigquery.SchemaField("lm_periods", "JSON"),
]

UPCOMING_MATCHES_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("matchup", "STRING"),
    bigquery.SchemaField("start_time_utc", "TIMESTAMP"),
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
        print(f"[match_info] Created dataset {DATASET}")
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
        print(f"[match_info] Created table {DATASET}.{table}")
    except Conflict:
        pass


def _truncate_and_insert(
    client: bigquery.Client,
    table: str,
    rows: List[Dict[str, Any]],
    schema: List[bigquery.SchemaField],
) -> int:
    if not rows:
        return 0

    import tempfile

    table_id = _full_table_id(client, table)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")
        tmp_path = f.name

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    with open(tmp_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)

    job.result()
    os.unlink(tmp_path)
    return len(rows)


# ── Shared helpers ────────────────────────────────────────────────────────────


def _safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ts(value: Optional[str]) -> Optional[str]:
    """Strip timezone offset for BigQuery TIMESTAMP."""
    if not value:
        return None
    return value.split("+")[0].strip()


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


# ── Row builders ──────────────────────────────────────────────────────────────


def _build_upcoming_match_rows(
    matches: List[Dict[str, Any]],
    *,
    ingested_at: str,
    scraped_date: str,
    now_utc: datetime,
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

        start_raw = match.get("date_utc") or info.get("starttime") or info.get("md")
        start_dt = _parse_start_time_utc(start_raw)
        if start_dt is None or start_dt < now_utc:
            continue

        row = {
            "ingested_at": ingested_at,
            "scraped_date": scraped_date,
            "match_id": match_id,
            "home_team": home_team,
            "away_team": away_team,
            "matchup": f"{home_team} vs {away_team}",
            "start_time_utc": _format_start_time_utc(start_dt),
        }
        existing = by_match_id.get(match_id)
        if existing is None or start_dt < existing[0]:
            by_match_id[match_id] = (start_dt, row)

    return [
        row
        for _, row in sorted(
            by_match_id.values(),
            key=lambda item: (item[0], item[1]["matchup"]),
        )
    ]


def _build_weather_row(
    match_id: int,
    data: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> Dict[str, Any]:
    weather = data.get("weather_conditions") or {}
    return {
        "ingested_at": ingested_at,
        "scraped_date": scraped_date,
        "match_id": match_id,
        "home_team": data.get("ht"),
        "away_team": data.get("at"),
        "date_utc": (data.get("starttime") or "").split("+")[0].strip() or None,
        "weather_icon": weather.get("icon"),
        "weather_temp_c": weather.get("temperature"),
        "home_form": data.get("ht_form"),
        "away_form": data.get("at_form"),
    }


def _build_key_rows(
    match_id: int,
    data: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows = []
    home_team = data.get("ht")
    away_team = data.get("at")
    date_utc = (data.get("starttime") or "").split("+")[0].strip() or None

    for rank, key in enumerate(data.get("match_keys") or [], start=1):
        if not isinstance(key, dict):
            continue
        rows.append({
            "ingested_at": ingested_at,
            "scraped_date": scraped_date,
            "match_id": match_id,
            "home_team": home_team,
            "away_team": away_team,
            "date_utc": date_utc,
            "rank": rank,
            "statement": key.get("statement"),
            "teams_json": json.dumps(key.get("teams") or []),
        })
    return rows


def _build_betting_stats_rows(
    match_id: int,
    match_info: Dict[str, Any],
    betting_stats: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows = []
    home_team = match_info.get("ht")
    away_team = match_info.get("at")
    date_utc = (match_info.get("starttime") or "").split("+")[0].strip() or None

    for category in (betting_stats.get("data") or []):
        category_label = category.get("label")

        for sub_tab in (category.get("data") or []):
            sub_tab_label = sub_tab.get("label")
            total_matches = sub_tab.get("total_matches") or {}

            for stat in (sub_tab.get("data") or []):
                if not isinstance(stat, dict):
                    continue
                if stat.get("label") == "scoring_minutes":
                    continue
                rows.append({
                    "ingested_at": ingested_at,
                    "scraped_date": scraped_date,
                    "match_id": match_id,
                    "home_team": home_team,
                    "away_team": away_team,
                    "date_utc": date_utc,
                    "category": category_label,
                    "sub_tab": sub_tab_label,
                    "label": stat.get("label"),
                    "value": str(stat["value"]) if stat.get("value") is not None else None,
                    "home": float(stat["home"]) if stat.get("home") is not None else None,
                    "away": float(stat["away"]) if stat.get("away") is not None else None,
                    "total_matches_home": total_matches.get("home"),
                    "total_matches_away": total_matches.get("away"),
                })
    return rows


def _build_last_match_rows(
    match_id: int,
    match_info: Dict[str, Any],
    last_matches_home: Dict[str, Any],
    last_matches_away: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows = []
    home_team = match_info.get("ht")
    away_team = match_info.get("at")
    date_utc = _ts(match_info.get("starttime") or match_info.get("md"))

    def _process(side: str, team_data: Dict[str, Any]) -> None:
        for m in (team_data.get("matches") or []):
            # Skip the upcoming match itself (no score yet)
            if m.get("matchstatus") == 1 and m.get("hscore") is None:
                continue
            rows.append({
                "ingested_at": ingested_at,
                "scraped_date": scraped_date,
                "match_id": match_id,
                "home_team": home_team,
                "away_team": away_team,
                "date_utc": date_utc,
                "side": side,
                "lm_match_id": m.get("id"),
                "lm_date": _ts(m.get("md")),
                "lm_ht": m.get("ht"),
                "lm_at": m.get("at"),
                "lm_ht_id": _safe_int(m.get("ht_id")),
                "lm_at_id": _safe_int(m.get("at_id")),
                "lm_hscore": _safe_int(m.get("hscore")),
                "lm_ascore": _safe_int(m.get("ascore")),
                "lm_outcome": m.get("outcome"),
                "lm_home": bool(m.get("home", False)),
                "lm_league_id": _safe_int(m.get("league_id")),
                "lm_matchstatus": _safe_int(m.get("matchstatus")),
                "lm_match_key": _safe_int(m.get("match_key")),
                "lm_periods": m.get("status"),  # JSON string of period scores
            })

    ht_id = str(match_info.get("ht_id", ""))
    at_id = str(match_info.get("at_id", ""))

    home_data = last_matches_home.get(ht_id) or {}
    away_data = last_matches_away.get(at_id) or {}

    _process("home", home_data)
    _process("away", away_data)
    return rows


# ── Main ingest ───────────────────────────────────────────────────────────────


def ingest_match_info(
    url: Optional[str] = None,
    *,
    dry_run: bool = False,
    scrape_only: bool = False,
    load_only: bool = False,
) -> Dict[str, Any]:
    target_url = url or ODDSPEDIA_URL
    now = datetime.now(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    print("=" * 60)
    print("[match_info] Starting Oddspedia MLS match info ingest")
    print(f"[match_info] URL         : {target_url}")
    print(f"[match_info] Ingested at : {ingested_at}")
    if dry_run:
        print("[match_info] Mode        : DRY RUN")
    elif scrape_only:
        print("[match_info] Mode        : SCRAPE ONLY (save to file)")
    elif load_only:
        print("[match_info] Mode        : LOAD ONLY (read from file)")
    print("=" * 60)

    # ── Load-only path ────────────────────────────────────────────────────────
    if load_only:
        weather_rows: List[Dict] = []
        key_rows: List[Dict] = []
        betting_stats_rows: List[Dict] = []
        last_match_rows: List[Dict] = []
        upcoming_match_rows: List[Dict] = []

        for table_name, target_list in [
            ("weather", weather_rows),
            ("keys", key_rows),
            ("betting_stats", betting_stats_rows),
            ("last_matches", last_match_rows),
            ("upcoming", upcoming_match_rows),
        ]:
            path = f"/tmp/mls_scrape_match_{table_name}.ndjson"
            try:
                with open(path) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            target_list.append(json.loads(line))
                print(f"[match_info] Loaded {len(target_list)} rows from {path}")
            except FileNotFoundError:
                print(f"[match_info] No file at {path} — skipping")

        bq = _bq_client()
        _ensure_dataset(bq)
        _ensure_table(bq, WEATHER_TABLE, WEATHER_SCHEMA)
        _ensure_table(bq, KEYS_TABLE, KEYS_SCHEMA)
        _ensure_table(bq, BETTING_STATS_TABLE, BETTING_STATS_SCHEMA)
        _ensure_table(bq, LAST_MATCHES_TABLE, LAST_MATCHES_SCHEMA)
        _ensure_table(bq, UPCOMING_MATCHES_TABLE, UPCOMING_MATCHES_SCHEMA)

        w_written = _truncate_and_insert(bq, WEATHER_TABLE, weather_rows, WEATHER_SCHEMA)
        k_written = _truncate_and_insert(bq, KEYS_TABLE, key_rows, KEYS_SCHEMA)
        b_written = _truncate_and_insert(bq, BETTING_STATS_TABLE, betting_stats_rows, BETTING_STATS_SCHEMA)
        lm_written = _truncate_and_insert(bq, LAST_MATCHES_TABLE, last_match_rows, LAST_MATCHES_SCHEMA)
        u_written = _truncate_and_insert(bq, UPCOMING_MATCHES_TABLE, upcoming_match_rows, UPCOMING_MATCHES_SCHEMA)

        print(
            f"[match_info] Written — weather: {w_written}, keys: {k_written}, "
            f"betting stats: {b_written}, last matches: {lm_written}, upcoming: {u_written}"
        )
        print("=" * 60)
        return {
            "weather_rows_written": w_written,
            "key_rows_written": k_written,
            "betting_stats_rows_written": b_written,
            "last_match_rows_written": lm_written,
            "upcoming_match_rows_written": u_written,
            "errors": [],
        }

    # ── Scrape ────────────────────────────────────────────────────────────────
    scraper = OddspediaClient()
    matches = scraper.scrape(target_url)
    upcoming_match_rows = _build_upcoming_match_rows(
        matches,
        ingested_at=ingested_at,
        scraped_date=scraped_date,
        now_utc=now,
    )
    today_matches = [
        m for m in matches
        if (m.get("date_utc") or "").startswith(scraped_date)
    ]
    print(f"[match_info] {len(today_matches)} matches today")

    weather_rows_out: List[Dict] = []
    key_rows_out: List[Dict] = []
    betting_stats_rows_out: List[Dict] = []
    last_match_rows_out: List[Dict] = []

    for match in today_matches:
        mid = match.get("match_id")
        if not mid:
            continue
        data = match.get("match_info") or {}
        if not data:
            print(f"[match_info] match={mid} no match_info on record — skipping")
            continue
        print(f"[match_info] match={mid} processing: {data.get('ht')} vs {data.get('at')}")

        weather_rows_out.append(_build_weather_row(mid, data, ingested_at, scraped_date))
        key_rows_out.extend(_build_key_rows(mid, data, ingested_at, scraped_date))

        betting_stats = match.get("betting_stats") or {}
        if betting_stats:
            rows = _build_betting_stats_rows(mid, data, betting_stats, ingested_at, scraped_date)
            betting_stats_rows_out.extend(rows)
            print(f"[match_info] match={mid} betting stats rows: {len(rows)}")
        else:
            print(f"[match_info] match={mid} no betting stats on record")

        lmh = match.get("last_matches_home") or {}
        lma = match.get("last_matches_away") or {}
        if lmh or lma:
            rows = _build_last_match_rows(mid, data, lmh, lma, ingested_at, scraped_date)
            last_match_rows_out.extend(rows)
            print(f"[match_info] match={mid} last match rows: {len(rows)}")
        else:
            print(f"[match_info] match={mid} no last matches on record")

    print(f"[match_info] Weather rows      : {len(weather_rows_out)}")
    print(f"[match_info] Key rows          : {len(key_rows_out)}")
    print(f"[match_info] Betting stat rows : {len(betting_stats_rows_out)}")
    print(f"[match_info] Last match rows   : {len(last_match_rows_out)}")
    print(f"[match_info] Upcoming rows     : {len(upcoming_match_rows)}")

    # ── Dry-run ───────────────────────────────────────────────────────────────
    if dry_run:
        print("\n--- WEATHER SAMPLE ---")
        print(json.dumps(weather_rows_out[:2], indent=2, default=str))
        print("\n--- KEYS SAMPLE ---")
        print(json.dumps(key_rows_out[:5], indent=2, default=str))
        print("\n--- BETTING STATS SAMPLE ---")
        print(json.dumps(betting_stats_rows_out[:5], indent=2, default=str))
        print("\n--- LAST MATCHES SAMPLE ---")
        print(json.dumps(last_match_rows_out[:5], indent=2, default=str))
        print("\n--- UPCOMING MATCHES SAMPLE ---")
        print(json.dumps(upcoming_match_rows[:10], indent=2, default=str))
        return {
            "weather_rows": len(weather_rows_out),
            "key_rows": len(key_rows_out),
            "betting_stats_rows": len(betting_stats_rows_out),
            "last_match_rows": len(last_match_rows_out),
            "upcoming_match_rows": len(upcoming_match_rows),
            "dry_run": True,
        }

    # ── Scrape-only: save to files and exit ───────────────────────────────────
    if scrape_only:
        for table_name, row_list in [
            ("weather", weather_rows_out),
            ("keys", key_rows_out),
            ("betting_stats", betting_stats_rows_out),
            ("last_matches", last_match_rows_out),
            ("upcoming", upcoming_match_rows),
        ]:
            path = f"/tmp/mls_scrape_match_{table_name}.ndjson"
            with open(path, "w") as f:
                for row in row_list:
                    f.write(json.dumps(row, default=str) + "\n")
            print(f"[match_info] Saved {len(row_list)} rows to {path}")
        print("=" * 60)
        return {
            "weather_rows": len(weather_rows_out),
            "key_rows": len(key_rows_out),
            "betting_stats_rows": len(betting_stats_rows_out),
            "last_match_rows": len(last_match_rows_out),
            "upcoming_match_rows": len(upcoming_match_rows),
            "errors": [],
        }

    # ── BigQuery setup + load (normal full run) ───────────────────────────────
    bq = _bq_client()
    _ensure_dataset(bq)
    _ensure_table(bq, WEATHER_TABLE, WEATHER_SCHEMA)
    _ensure_table(bq, KEYS_TABLE, KEYS_SCHEMA)
    _ensure_table(bq, BETTING_STATS_TABLE, BETTING_STATS_SCHEMA)
    _ensure_table(bq, LAST_MATCHES_TABLE, LAST_MATCHES_SCHEMA)
    _ensure_table(bq, UPCOMING_MATCHES_TABLE, UPCOMING_MATCHES_SCHEMA)

    w_written = _truncate_and_insert(bq, WEATHER_TABLE, weather_rows_out, WEATHER_SCHEMA)
    k_written = _truncate_and_insert(bq, KEYS_TABLE, key_rows_out, KEYS_SCHEMA)
    b_written = _truncate_and_insert(bq, BETTING_STATS_TABLE, betting_stats_rows_out, BETTING_STATS_SCHEMA)
    lm_written = _truncate_and_insert(bq, LAST_MATCHES_TABLE, last_match_rows_out, LAST_MATCHES_SCHEMA)
    u_written = _truncate_and_insert(bq, UPCOMING_MATCHES_TABLE, upcoming_match_rows, UPCOMING_MATCHES_SCHEMA)

    print(
        f"[match_info] Written — weather: {w_written}, keys: {k_written}, "
        f"betting stats: {b_written}, last matches: {lm_written}, upcoming: {u_written}"
    )
    print("=" * 60)

    return {
        "weather_rows_written": w_written,
        "key_rows_written": k_written,
        "betting_stats_rows_written": b_written,
        "last_match_rows_written": lm_written,
        "upcoming_match_rows_written": u_written,
        "errors": [],
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="Scrape and save rows to /tmp/mls_scrape_match_*.ndjson. No BQ write.",
    )
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Load rows from /tmp/mls_scrape_match_*.ndjson into BigQuery. No scraping.",
    )
    args = parser.parse_args()

    result = ingest_match_info(
        url=args.url,
        dry_run=args.dry_run,
        scrape_only=args.scrape_only,
        load_only=args.load_only,
    )
    print(json.dumps(result, indent=2, default=str))