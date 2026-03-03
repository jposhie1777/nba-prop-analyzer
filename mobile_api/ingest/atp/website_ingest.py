from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from google.api_core import exceptions as gcp_exceptions
from google.api_core.retry import Retry
from google.cloud import bigquery

from atp_client import ATPClient
from atp_normalize import (
    normalize_calendar,
    normalize_head_to_head,
    normalize_match_results_html,
    normalize_match_schedule_html,
    normalize_overview,
    normalize_top_seeds,
    utc_now_iso,
)


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _dataset() -> str:
    return os.getenv("ATP_DATASET", "atp_data")


def _table(name: str) -> str:
    return f"{_dataset()}.{name}"


def _slug_from_overview_url(overview_url: Optional[str]) -> Optional[str]:
    if not overview_url:
        return None
    m = re.search(r"/en/tournaments/([^/]+)/\d+/overview", overview_url)
    return m.group(1) if m else None


def _extract_h2h_pairs(results_rows: Sequence[Dict[str, Any]]) -> Set[Tuple[str, str]]:
    pairs: Set[Tuple[str, str]] = set()
    for row in results_rows:
        h2h_url = row.get("h2h_url") or ""
        m = re.search(r"/atp-head-2-head/.+?/([a-z0-9]+)/([a-z0-9]+)$", h2h_url, flags=re.IGNORECASE)
        if m:
            pairs.add((m.group(1).upper(), m.group(2).upper()))
    return pairs


def _truncate_tables(client: bigquery.Client) -> None:
    for t in [
        "atp_raw_responses",
        "atp_tournament_months",
        "atp_tournaments",
        "atp_tournaments_overview",
        "atp_top_seeds",
        "atp_h2h_matches",
        "atp_match_schedule",
        "atp_match_results",
    ]:
        client.query(f"TRUNCATE TABLE `{_table(t)}`").result()


def _chunked(rows: Sequence[Dict[str, Any]], size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    for idx in range(0, len(rows), size):
        yield rows[idx : idx + size]


def _row_insert_id(row: Dict[str, Any], idx: int) -> str:
    encoded = json.dumps(row, sort_keys=True, default=str).encode("utf-8")
    return f"{hashlib.md5(encoded).hexdigest()}-{idx}"


def _insert_rows(client: bigquery.Client, table_name: str, rows: Iterable[Dict[str, Any]]) -> int:
    rows_list = list(rows)
    if not rows_list:
        return 0

    batch_size = int(os.getenv("ATP_BQ_INSERT_BATCH_SIZE", "200"))
    max_attempts = int(os.getenv("ATP_BQ_INSERT_MAX_ATTEMPTS", "8"))
    base_sleep = float(os.getenv("ATP_BQ_INSERT_RETRY_SECONDS", "2"))

    inserted = 0
    for batch_number, batch in enumerate(_chunked(rows_list, batch_size), start=1):
        row_ids = [_row_insert_id(row, idx) for idx, row in enumerate(batch)]

        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                errors = client.insert_rows_json(
                    _table(table_name),
                    list(batch),
                    row_ids=row_ids,
                    retry=Retry(deadline=60),
                )
                if errors:
                    raise RuntimeError(
                        f"BigQuery insert row errors for table={table_name} batch={batch_number}: {errors}"
                    )
                inserted += len(batch)
                last_exc = None
                break
            except (
                gcp_exceptions.RetryError,
                gcp_exceptions.ServiceUnavailable,
                gcp_exceptions.InternalServerError,
                gcp_exceptions.TooManyRequests,
                ConnectionError,
            ) as exc:
                last_exc = exc
                if attempt < max_attempts:
                    time.sleep(base_sleep * (2 ** (attempt - 1)))
            except Exception as exc:
                last_exc = exc
                if attempt < max_attempts:
                    time.sleep(base_sleep * (2 ** (attempt - 1)))

        if last_exc is not None:
            raise RuntimeError(
                f"Failed insert_rows_json for table={table_name} batch={batch_number} after {max_attempts} attempts"
            ) from last_exc

    return inserted


def _raw_row(snapshot_ts: str, ingest_run_id: str, endpoint_key: str, response: Dict[str, Any], payload_key: str) -> Dict[str, Any]:
    payload = response.get(payload_key)
    return {
        "snapshot_ts_utc": snapshot_ts,
        "ingest_run_id": ingest_run_id,
        "endpoint_key": endpoint_key,
        "url": response.get("url"),
        "status_code": response.get("status_code"),
        "is_not_modified": response.get("is_not_modified"),
        "etag": response.get("etag"),
        "last_modified": response.get("last_modified"),
        "content_type": response.get("content_type"),
        "payload_json": json.dumps(payload, ensure_ascii=False) if payload_key == "fetched_json" else None,
        "payload_text": payload if payload_key == "fetched_text" else None,
    }


def run_ingest(start_year: int, end_year: int, truncate: bool, sleep_seconds: float) -> Dict[str, Any]:
    snapshot_ts = utc_now_iso()
    ingest_run_id = str(uuid.uuid4())
    client = _bq_client()
    atp = ATPClient(timeout=25)

    if truncate:
        _truncate_tables(client)

    raw_rows: List[Dict[str, Any]] = []
    month_rows: List[Dict[str, Any]] = []
    tournament_rows: List[Dict[str, Any]] = []
    overview_rows: List[Dict[str, Any]] = []
    top_seed_rows: List[Dict[str, Any]] = []
    schedule_rows: List[Dict[str, Any]] = []
    results_rows: List[Dict[str, Any]] = []
    h2h_rows: List[Dict[str, Any]] = []

    calendar_resp = atp.fetch_calendar()
    calendar_json = calendar_resp["fetched_json"]
    raw_rows.append(_raw_row(snapshot_ts, ingest_run_id, "calendar", calendar_resp, "fetched_json"))

    m_rows, t_rows = normalize_calendar(calendar_json, snapshot_ts_utc=snapshot_ts)
    month_rows.extend([r.to_dict() for r in m_rows])
    tournament_rows.extend([r.to_dict() for r in t_rows])

    tournaments_for_year = [
        r for r in tournament_rows if any(str(y) in (r.get("formatted_date") or "") for y in range(start_year, end_year + 1))
    ]
    seen_tournament_ids: Set[str] = set()

    for t in tournaments_for_year:
        tid = t["tournament_id"]
        if tid in seen_tournament_ids:
            continue
        seen_tournament_ids.add(tid)

        overview_resp = atp.fetch_tournament_overview(tid)
        raw_rows.append(_raw_row(snapshot_ts, ingest_run_id, f"overview:{tid}", overview_resp, "fetched_json"))
        overview_json = overview_resp["fetched_json"]
        overview_rows.append(normalize_overview(tid, overview_json, snapshot_ts_utc=snapshot_ts, include_raw_json=True).to_dict())

        slug = _slug_from_overview_url(t.get("overview_url"))

        for year in range(start_year, end_year + 1):
            try:
                top_resp = atp.fetch_tournament_top_seeds(tid, year)
                raw_rows.append(_raw_row(snapshot_ts, ingest_run_id, f"topseeds:{tid}:{year}", top_resp, "fetched_json"))
                top_seed_rows.extend([r.to_dict() for r in normalize_top_seeds(tid, year, top_resp["fetched_json"], snapshot_ts_utc=snapshot_ts)])
            except Exception:
                pass

        if slug:
            try:
                sched_resp = atp.fetch_match_schedule_html(slug, tid)
                raw_rows.append(_raw_row(snapshot_ts, ingest_run_id, f"schedule:{slug}:{tid}", sched_resp, "fetched_text"))
                schedule_rows.extend([
                    r.to_dict() for r in normalize_match_schedule_html(slug, tid, sched_resp["fetched_text"], snapshot_ts_utc=snapshot_ts)
                ])
            except Exception:
                pass

            try:
                results_resp = atp.fetch_match_results_html(slug, tid)
                raw_rows.append(_raw_row(snapshot_ts, ingest_run_id, f"results:{slug}:{tid}", results_resp, "fetched_text"))
                parsed_results = [
                    r.to_dict() for r in normalize_match_results_html(slug, tid, results_resp["fetched_text"], snapshot_ts_utc=snapshot_ts)
                ]
                results_rows.extend(parsed_results)

                for left_id, right_id in sorted(_extract_h2h_pairs(parsed_results)):
                    try:
                        h2h_resp = atp.fetch_head_to_head(left_id, right_id)
                        raw_rows.append(_raw_row(snapshot_ts, ingest_run_id, f"h2h:{left_id}:{right_id}", h2h_resp, "fetched_json"))
                        h2h_rows.extend([
                            r.to_dict()
                            for r in normalize_head_to_head(left_id, right_id, h2h_resp["fetched_json"], snapshot_ts_utc=snapshot_ts)
                        ])
                    except Exception:
                        pass
            except Exception:
                pass

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    for row in overview_rows:
        if isinstance(row.get("raw_overview_json"), dict):
            row["raw_overview_json"] = json.dumps(row["raw_overview_json"], ensure_ascii=False)

    written = {
        "raw": _insert_rows(client, "atp_raw_responses", raw_rows),
        "months": _insert_rows(client, "atp_tournament_months", month_rows),
        "tournaments": _insert_rows(client, "atp_tournaments", tournament_rows),
        "overviews": _insert_rows(client, "atp_tournaments_overview", overview_rows),
        "top_seeds": _insert_rows(client, "atp_top_seeds", top_seed_rows),
        "schedule": _insert_rows(client, "atp_match_schedule", schedule_rows),
        "results": _insert_rows(client, "atp_match_results", results_rows),
        "h2h": _insert_rows(client, "atp_h2h_matches", h2h_rows),
    }

    return {
        "snapshot_ts_utc": snapshot_ts,
        "ingest_run_id": ingest_run_id,
        "start_year": start_year,
        "end_year": end_year,
        "truncate": truncate,
        "written": written,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="ATP website ingestion for BigQuery")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--years", type=int, default=5)
    parser.add_argument("--truncate", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--sleep", type=float, default=0.2)
    args = parser.parse_args()

    now_year = datetime.now(timezone.utc).year
    if args.mode == "daily":
        start_year = args.start_year or now_year
        end_year = args.end_year or now_year
        truncate = False
    else:
        end_year = args.end_year or now_year
        start_year = args.start_year if args.start_year is not None else max(1990, end_year - args.years + 1)
        truncate = bool(args.truncate)

    result = run_ingest(start_year=start_year, end_year=end_year, truncate=truncate, sleep_seconds=args.sleep)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
