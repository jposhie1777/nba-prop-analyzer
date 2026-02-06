from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

from bq import get_bq_client
from pga.client import PgaApiError, fetch_paginated
from pga.utils import parse_iso_datetime


DATASET = os.getenv("PGA_DATASET", "pga_data")
ROUND_SCORES_TABLE = os.getenv("PGA_ROUND_SCORES_TABLE", "tournament_round_scores")
DATASET_LOCATION = os.getenv("PGA_DATASET_LOCATION", "US")
ROUND_SCORES_SOURCE = os.getenv("PGA_ROUND_SCORES_SOURCE", "api")

NY_TZ = ZoneInfo("America/New_York")


SCHEMA_ROUND_SCORES = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("tournament_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("tournament_name", "STRING"),
    bigquery.SchemaField("tournament_start_date", "TIMESTAMP"),
    bigquery.SchemaField("round_number", "INT64"),
    bigquery.SchemaField("round_date", "DATE"),
    bigquery.SchemaField("player_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("player_display_name", "STRING"),
    bigquery.SchemaField("round_score", "INT64"),
    bigquery.SchemaField("par_relative_score", "INT64"),
    bigquery.SchemaField("total_score", "INT64"),
]


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _resolve_table_id(table: str, project: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{project}.{table}"
    return f"{project}.{DATASET}.{table}"


def _dataset_id(table_id: str) -> str:
    parts = table_id.split(".")
    if len(parts) < 2:
        raise ValueError(f"Invalid table id: {table_id}")
    return ".".join(parts[:2])


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = DATASET_LOCATION
        client.create_dataset(dataset)
    except Conflict:
        return


def ensure_table(client: bigquery.Client, table_id: str) -> None:
    try:
        client.get_table(table_id)
    except NotFound:
        table = bigquery.Table(table_id, schema=SCHEMA_ROUND_SCORES)
        table.range_partitioning = bigquery.RangePartitioning(
            field="season",
            range_=bigquery.PartitionRange(start=2015, end=2035, interval=1),
        )
        table.clustering_fields = ["tournament_id", "player_id", "round_number"]
        client.create_table(table)
    except Conflict:
        return


def insert_rows(
    client: bigquery.Client,
    table: str,
    rows: List[Dict[str, Any]],
    *,
    batch_size: int = 500,
) -> int:
    if not rows:
        return 0
    table_id = _resolve_table_id(table, client.project)
    total = 0
    for idx in range(0, len(rows), batch_size):
        batch = rows[idx : idx + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
        total += len(batch)
        time.sleep(0.05)
    return total


def _fetch_paginated_retry(
    path: str,
    params: Dict[str, Any],
    *,
    per_page: int = 100,
    max_pages: int = 50,
    source: str = "api",
) -> List[Dict[str, Any]]:
    backoff = 4
    for _ in range(4):
        try:
            return fetch_paginated(
                path,
                params=params,
                per_page=per_page,
                max_pages=max_pages,
                cache_ttl=0,
                source=source,
            )
        except PgaApiError as exc:
            message = str(exc)
            if "429" in message or "503" in message:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
    raise RuntimeError(f"Exceeded retries for {path}")


def _tournament_dates(tournament: Dict[str, Any]) -> tuple[Optional[date], Optional[date]]:
    start = parse_iso_datetime(tournament.get("start_date"))
    end = parse_iso_datetime(tournament.get("end_date"))
    return (start.date() if start else None, end.date() if end else None)


def _infer_round_number(start_date: Optional[date], target_date: date) -> Optional[int]:
    if not start_date:
        return None
    delta = (target_date - start_date).days
    if delta < 0:
        return None
    round_number = delta + 1
    return round_number if 1 <= round_number <= 6 else None


def _active_tournaments_for_date(
    tournaments: Iterable[Dict[str, Any]],
    *,
    target_date: date,
) -> List[Dict[str, Any]]:
    active: List[Dict[str, Any]] = []
    for tournament in tournaments:
        start, end = _tournament_dates(tournament)
        status = (tournament.get("status") or "").strip().lower()
        if start and end and start <= target_date <= end:
            active.append(tournament)
            continue
        if status in {"in_progress", "active", "ongoing", "live"}:
            active.append(tournament)
    return active


def _fetch_rounds_for_tournament(
    tournament_id: int,
    *,
    target_date: Optional[date],
    round_number: Optional[int],
    source: str,
) -> tuple[List[Dict[str, Any]], bool]:
    base = {"tournament_ids": [tournament_id]}
    attempts: List[Dict[str, Any]] = []
    if round_number is not None and target_date is not None:
        attempts.append({**base, "round_number": round_number, "date": target_date.isoformat()})
    if round_number is not None:
        attempts.append({**base, "round_number": round_number})
    if target_date is not None:
        attempts.append({**base, "date": target_date.isoformat()})
    attempts.append(base)

    for params in attempts:
        try:
            records = _fetch_paginated_retry("/tournament_rounds", params, source=source)
        except PgaApiError as exc:
            message = str(exc)
            if "404" in message and "Route not found" in message:
                results = _fetch_paginated_retry("/tournament_results", base, source=source)
                return results, True
            raise
        if records:
            return records, False

    if round_number is not None:
        alt_params = {**base, "round": round_number}
        if target_date is not None:
            alt_params["date"] = target_date.isoformat()
        records = _fetch_paginated_retry("/tournament_rounds", alt_params, source=source)
        return records, False
    return [], False


def normalize_round_scores(
    records: Iterable[Dict[str, Any]],
    *,
    run_ts: str,
    fallback_date: date,
    tournament_lookup: Dict[int, Dict[str, Any]],
    round_number_filter: Optional[int] = None,
    skip_round_filter_when_missing: bool = False,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for record in records:
        tournament = record.get("tournament") or {}
        player = record.get("player") or {}
        tournament_id = _parse_int(tournament.get("id") or record.get("tournament_id"))
        player_id = _parse_int(player.get("id") or record.get("player_id"))
        if not tournament_id or not player_id:
            continue
        fallback_tournament = tournament_lookup.get(tournament_id) or {}

        round_number = _parse_int(
            record.get("round_number") or record.get("round") or record.get("round_num")
        )
        if round_number_filter is not None and round_number != round_number_filter:
            if not (skip_round_filter_when_missing and round_number is None):
                continue

        round_date = _parse_date(
            record.get("round_date")
            or record.get("date")
            or record.get("played_at")
            or record.get("round_start_date")
        ) or fallback_date

        round_score = _parse_int(
            record.get("round_score") or record.get("score") or record.get("strokes")
        )
        par_relative_score = _parse_int(
            record.get("par_relative_score")
            or record.get("score_to_par")
            or record.get("relative_to_par")
        )
        total_score = _parse_int(
            record.get("total_score") or record.get("cumulative_score") or record.get("total_strokes")
        )

        rows.append(
            {
                "run_ts": run_ts,
                "ingested_at": run_ts,
                "season": tournament.get("season") or fallback_tournament.get("season"),
                "tournament_id": tournament_id,
                "tournament_name": tournament.get("name") or fallback_tournament.get("name"),
                "tournament_start_date": tournament.get("start_date")
                or fallback_tournament.get("start_date"),
                "round_number": round_number,
                "round_date": round_date.isoformat() if round_date else None,
                "player_id": player_id,
                "player_display_name": player.get("display_name")
                or record.get("player_display_name"),
                "round_score": round_score,
                "par_relative_score": par_relative_score,
                "total_score": total_score,
            }
        )
    return rows


def ingest_round_scores(
    *,
    target_date: Optional[str] = None,
    season: Optional[int] = None,
    tournament_id: Optional[int] = None,
    round_number: Optional[int] = None,
    create_tables: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if target_date:
        target_day = date.fromisoformat(target_date)
    else:
        target_day = datetime.now(NY_TZ).date() - timedelta(days=1)

    season = season or target_day.year
    source = ROUND_SCORES_SOURCE

    tournaments = _fetch_paginated_retry(
        "/tournaments",
        params={"season": season},
        per_page=100,
        max_pages=50,
        source=source,
    )

    tournament_lookup = {t.get("id"): t for t in tournaments if t.get("id")}

    if tournament_id:
        target_tournaments = [tournament_lookup.get(tournament_id) or {"id": tournament_id}]
    else:
        target_tournaments = _active_tournaments_for_date(
            tournaments,
            target_date=target_day,
        )

    run_ts = _now_iso()
    summary: Dict[str, Any] = {
        "target_date": target_day.isoformat(),
        "season": season,
        "tournament_count": len(target_tournaments),
        "round_number": round_number,
        "records": 0,
        "inserted": 0,
    }

    all_rows: List[Dict[str, Any]] = []
    for tournament in target_tournaments:
        tid = _parse_int(tournament.get("id"))
        if not tid:
            continue
        start_date, _ = _tournament_dates(tournament)
        inferred_round = _infer_round_number(start_date, target_day)
        round_to_fetch = round_number or inferred_round

        records, used_results_fallback = _fetch_rounds_for_tournament(
            tid,
            target_date=target_day,
            round_number=round_to_fetch,
            source=source,
        )
        rows = normalize_round_scores(
            records,
            run_ts=run_ts,
            fallback_date=target_day,
            tournament_lookup=tournament_lookup,
            round_number_filter=round_to_fetch,
            skip_round_filter_when_missing=used_results_fallback,
        )
        summary.setdefault("tournaments", {})[str(tid)] = {
            "round_number": round_to_fetch,
            "records": len(records),
            "rows": len(rows),
            "used_results_fallback": used_results_fallback,
        }
        all_rows.extend(rows)

    summary["records"] = len(all_rows)
    if dry_run:
        summary["inserted"] = 0
        return summary

    client = get_bq_client()
    table_id = _resolve_table_id(ROUND_SCORES_TABLE, client.project)
    if create_tables:
        ensure_dataset(client, _dataset_id(table_id))
        ensure_table(client, table_id)

    summary["inserted"] = insert_rows(client, ROUND_SCORES_TABLE, all_rows)
    return summary
