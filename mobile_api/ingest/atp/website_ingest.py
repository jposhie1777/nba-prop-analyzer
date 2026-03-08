from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from google.api_core import exceptions as gcp_exceptions
from google.api_core.retry import Retry
from google.cloud import bigquery

from atp_normalize import (
    normalize_calendar,
    normalize_head_to_head,
    normalize_match_results_html,
    normalize_match_schedule_html,
    utc_now_iso,
)


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _dataset() -> str:
    return os.getenv("ATP_DATASET", "atp_data")


def _table(name: str) -> str:
    return f"{_dataset()}.{name}"


def _truncate_tables(client: bigquery.Client) -> None:
    for t in [
        "website_raw_responses",
        "website_tournament_months",
        "website_tournaments",
        "website_daily_schedule",
        "website_upcoming_matches",
        "website_draws",
        "website_tournament_bracket",
        "website_head_to_head",
        "website_head_to_head_matches",
        "website_match_results",
        "website_match_results_rows",
        "website_player_stats",
        "website_player_stats_records",
        "website_who_is_playing",
        "website_who_is_playing_players",
    ]:
        client.query(f"TRUNCATE TABLE `{_table(t)}`").result()


def _truncate_table(client: bigquery.Client, table_name: str) -> None:
    client.query(f"TRUNCATE TABLE `{_table(table_name)}`").result()


def _chunked(rows: Sequence[Dict[str, Any]], size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    for idx in range(0, len(rows), size):
        yield rows[idx : idx + size]


def _row_insert_id(row: Dict[str, Any], idx: int) -> str:
    encoded = json.dumps(row, sort_keys=True, default=str).encode("utf-8")
    return f"{hashlib.md5(encoded).hexdigest()}-{idx}"


def _load_rows_with_job(client: bigquery.Client, table_name: str, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=False,
    )
    job = client.load_table_from_json(rows, _table(table_name), job_config=job_config)
    job.result()


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
            try:
                _load_rows_with_job(client, table_name, batch)
                inserted += len(batch)
            except Exception as load_exc:
                raise RuntimeError(
                    f"Failed insert_rows_json for table={table_name} batch={batch_number} after {max_attempts} attempts; "
                    "load job fallback also failed"
                ) from load_exc

    return inserted


def _extract_request_url(header_text: str) -> Optional[str]:
    for line in header_text.splitlines():
        value = line.strip()
        if value.lower().startswith("https://") or value.lower().startswith("http://"):
            return value
    return None


def _load_capture_file(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    if text.startswith("<!--"):
        end_idx = text.find("-->")
        if end_idx < 0:
            raise ValueError(f"Missing closing HTML comment marker in {path}")
        header = text[4:end_idx].strip()
        payload = text[end_idx + 3 :].strip()
        content_type = "text/html"
    elif text.startswith('"""'):
        delimiter = '"""\n\n'
        split_idx = text.find(delimiter)
        if split_idx < 0:
            raise ValueError(f"Missing triple quote delimiter in {path}")
        header = text[3:split_idx].strip()
        payload = text[split_idx + len(delimiter) :].strip()
        content_type = "application/json"
    else:
        raise ValueError(f"Unsupported file wrapper in {path}")

    request_url = _extract_request_url(header)
    return {
        "source_file": str(path),
        "endpoint_key": path.name,
        "request_url": request_url,
        "header_text": header,
        "content_type": content_type,
        "payload_text": payload,
        "payload_json": json.loads(payload) if content_type == "application/json" else None,
    }


def _raw_row(snapshot_ts: str, ingest_run_id: str, response: Dict[str, Any]) -> Dict[str, Any]:
    payload_json = response.get("payload_json")
    payload_text = response.get("payload_text")
    return {
        "snapshot_ts_utc": snapshot_ts,
        "ingest_run_id": ingest_run_id,
        "endpoint_key": response.get("endpoint_key"),
        "source_file": response.get("source_file"),
        "url": response.get("request_url"),
        "content_type": response.get("content_type"),
        "request_header": response.get("header_text"),
        "payload_json": json.dumps(payload_json, ensure_ascii=False) if payload_json is not None else None,
        "payload_text": payload_text if payload_json is None else None,
    }


def _extract_slug_and_tournament_id(url: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not url:
        return None, None
    m = re.search(r"/scores/current/([^/]+)/([^/]+)/", url)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def _extract_h2h_ids(url: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not url:
        return None, None
    m = re.search(r"/h2h/([^/]+)/([^/?#]+)", url)
    if not m:
        return None, None
    return m.group(1).upper(), m.group(2).upper()


def _safe_json_str(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _flatten_html_payload(payload_html: Optional[str]) -> Tuple[List[str], List[Dict[str, Optional[str]]]]:
    if not payload_html:
        return [], []

    text_only = re.sub(r"<script[\s\S]*?</script>", " ", payload_html, flags=re.IGNORECASE)
    text_only = re.sub(r"<style[\s\S]*?</style>", " ", text_only, flags=re.IGNORECASE)
    text_only = re.sub(r"<[^>]+>", " ", text_only)
    text_only = re.sub(r"\s+", " ", text_only)

    text_chunks: List[str] = []
    for chunk in re.split(r"[\r\n]+", text_only):
        cleaned = chunk.strip()
        if cleaned:
            text_chunks.append(cleaned)

    links: List[Dict[str, Optional[str]]] = []
    for m in re.finditer(
        r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        payload_html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        link_text = re.sub(r"<[^>]+>", " ", m.group(2))
        link_text = re.sub(r"\s+", " ", link_text).strip()
        links.append({"href": m.group(1).strip(), "text": link_text or None})

    return text_chunks[:500], links[:500]


def run_ingest(start_year: int, end_year: int, truncate: bool, truncate_schedule: bool, sleep_seconds: float) -> Dict[str, Any]:
    del sleep_seconds

    snapshot_ts = utc_now_iso()
    ingest_run_id = str(uuid.uuid4())
    client = _bq_client()

    responses_root = Path(os.getenv("ATP_WEBSITE_RESPONSES_DIR", "website_responses/atp"))
    endpoint_files = {
        "daily_schedule": responses_root / "daily_schedule",
        "draws": responses_root / "draws",
        "head_to_head": responses_root / "head_to_head",
        "match_results": responses_root / "match_results",
        "player_stats_all": responses_root / "player_stats_all",
        "player_stats_clay": responses_root / "player_stats_clay",
        "player_stats_grass": responses_root / "player_stats_grass",
        "player_stats_hard": responses_root / "player_stats_hard",
        "tournament_dates": responses_root / "tournament_dates",
        "who_is_playing": responses_root / "who_is_playing",
    }

    if truncate:
        _truncate_tables(client)
    if truncate_schedule:
        _truncate_table(client, "website_daily_schedule")
        _truncate_table(client, "website_upcoming_matches")

    captures = {key: _load_capture_file(path) for key, path in endpoint_files.items()}

    raw_rows: List[Dict[str, Any]] = [_raw_row(snapshot_ts, ingest_run_id, resp) for resp in captures.values()]

    tournament_month_rows: List[Dict[str, Any]] = []
    tournament_rows: List[Dict[str, Any]] = []
    daily_schedule_rows: List[Dict[str, Any]] = []
    upcoming_match_rows: List[Dict[str, Any]] = []
    draws_rows: List[Dict[str, Any]] = []
    bracket_rows: List[Dict[str, Any]] = []
    h2h_rows: List[Dict[str, Any]] = []
    h2h_match_rows: List[Dict[str, Any]] = []
    match_results_rows: List[Dict[str, Any]] = []
    parsed_match_results_rows: List[Dict[str, Any]] = []
    player_stats_rows: List[Dict[str, Any]] = []
    player_stats_records_rows: List[Dict[str, Any]] = []
    who_is_playing_rows: List[Dict[str, Any]] = []
    who_is_playing_players_rows: List[Dict[str, Any]] = []

    tournament_dates = captures["tournament_dates"]
    if isinstance(tournament_dates["payload_json"], dict):
        month_models, tournament_models = normalize_calendar(tournament_dates["payload_json"], snapshot_ts_utc=snapshot_ts)
        tournament_month_rows.extend([r.to_dict() for r in month_models])
        tournament_rows.extend([r.to_dict() for r in tournament_models])
        if start_year and end_year:
            tournament_rows = [
                row
                for row in tournament_rows
                if any(str(y) in (row.get("formatted_date") or "") for y in range(start_year, end_year + 1))
            ]

    daily_schedule_capture = captures["daily_schedule"]
    daily_text_chunks, daily_links = _flatten_html_payload(daily_schedule_capture.get("payload_text"))
    daily_schedule_rows.append(
        {
            "snapshot_ts_utc": snapshot_ts,
            "ingest_run_id": ingest_run_id,
            "url": daily_schedule_capture.get("request_url"),
            "payload_html": daily_schedule_capture.get("payload_text"),
            "flattened_text_chunks": daily_text_chunks,
            "flattened_links": daily_links,
        }
    )
    sched_slug, sched_tid = _extract_slug_and_tournament_id(daily_schedule_capture.get("request_url"))
    if sched_slug and sched_tid and daily_schedule_capture.get("payload_text"):
        upcoming_match_rows.extend(
            [
                row.to_dict()
                for row in normalize_match_schedule_html(
                    sched_slug,
                    sched_tid,
                    daily_schedule_capture["payload_text"],
                    snapshot_ts_utc=snapshot_ts,
                )
            ]
        )

    draws_capture = captures["draws"]
    draws_text_chunks, draws_links = _flatten_html_payload(draws_capture.get("payload_text"))
    draws_rows.append(
        {
            "snapshot_ts_utc": snapshot_ts,
            "ingest_run_id": ingest_run_id,
            "url": draws_capture.get("request_url"),
            "payload_html": draws_capture.get("payload_text"),
            "flattened_text_chunks": draws_text_chunks,
            "flattened_links": draws_links,
        }
    )
    bracket_rows.append(
        {
            "snapshot_ts_utc": snapshot_ts,
            "ingest_run_id": ingest_run_id,
            "tournament_slug": _extract_slug_and_tournament_id(draws_capture.get("request_url"))[0],
            "tournament_id": _extract_slug_and_tournament_id(draws_capture.get("request_url"))[1],
            "bracket_html": draws_capture.get("payload_text"),
        }
    )

    h2h_capture = captures["head_to_head"]
    h2h_rows.append(
        {
            "snapshot_ts_utc": snapshot_ts,
            "ingest_run_id": ingest_run_id,
            "url": h2h_capture.get("request_url"),
            "payload_json": _safe_json_str(h2h_capture.get("payload_json")),
        }
    )
    left_id, right_id = _extract_h2h_ids(h2h_capture.get("request_url"))
    if left_id and right_id and isinstance(h2h_capture.get("payload_json"), dict):
        h2h_match_rows.extend(
            [
                row.to_dict()
                for row in normalize_head_to_head(left_id, right_id, h2h_capture["payload_json"], snapshot_ts_utc=snapshot_ts)
            ]
        )

    results_capture = captures["match_results"]
    results_text_chunks, results_links = _flatten_html_payload(results_capture.get("payload_text"))
    match_results_rows.append(
        {
            "snapshot_ts_utc": snapshot_ts,
            "ingest_run_id": ingest_run_id,
            "url": results_capture.get("request_url"),
            "payload_html": results_capture.get("payload_text"),
            "flattened_text_chunks": results_text_chunks,
            "flattened_links": results_links,
        }
    )
    result_slug, result_tid = _extract_slug_and_tournament_id(results_capture.get("request_url"))
    if result_slug and result_tid and results_capture.get("payload_text"):
        parsed_match_results_rows.extend(
            [
                row.to_dict()
                for row in normalize_match_results_html(
                    result_slug,
                    result_tid,
                    results_capture["payload_text"],
                    snapshot_ts_utc=snapshot_ts,
                )
            ]
        )

    for endpoint, court_type in [
        ("player_stats_all", "all"),
        ("player_stats_clay", "clay"),
        ("player_stats_grass", "grass"),
        ("player_stats_hard", "hard"),
    ]:
        capture = captures[endpoint]
        payload_json = capture.get("payload_json")
        player_stats_rows.append(
            {
                "snapshot_ts_utc": snapshot_ts,
                "ingest_run_id": ingest_run_id,
                "court_type": court_type,
                "url": capture.get("request_url"),
                "payload_json": _safe_json_str(payload_json),
            }
        )
        if isinstance(payload_json, dict):
            stats_block = payload_json.get("FirstStats") or {}
            for stat_name, stat_value in stats_block.items():
                player_stats_records_rows.append(
                    {
                        "snapshot_ts_utc": snapshot_ts,
                        "ingest_run_id": ingest_run_id,
                        "court_type": court_type,
                        "stat_name": str(stat_name),
                        "stat_value": json.dumps(stat_value, ensure_ascii=False) if isinstance(stat_value, (dict, list)) else str(stat_value),
                    }
                )

    who_capture = captures["who_is_playing"]
    who_payload = who_capture.get("payload_json")
    who_is_playing_rows.append(
        {
            "snapshot_ts_utc": snapshot_ts,
            "ingest_run_id": ingest_run_id,
            "url": who_capture.get("request_url"),
            "payload_json": _safe_json_str(who_payload),
        }
    )
    if isinstance(who_payload, dict):
        for player in who_payload.get("PlayersList", []) or []:
            who_is_playing_players_rows.append(
                {
                    "snapshot_ts_utc": snapshot_ts,
                    "ingest_run_id": ingest_run_id,
                    "first_name": player.get("FirstName"),
                    "last_name": player.get("LastName"),
                    "profile_url": player.get("ProfileUrl"),
                    "country_flag_url": player.get("CountryFlagUrl"),
                }
            )

    written = {
        "raw": _insert_rows(client, "website_raw_responses", raw_rows),
        "tournament_months": _insert_rows(client, "website_tournament_months", tournament_month_rows),
        "tournaments": _insert_rows(client, "website_tournaments", tournament_rows),
        "daily_schedule": _insert_rows(client, "website_daily_schedule", daily_schedule_rows),
        "upcoming_matches": _insert_rows(client, "website_upcoming_matches", upcoming_match_rows),
        "draws": _insert_rows(client, "website_draws", draws_rows),
        "tournament_bracket": _insert_rows(client, "website_tournament_bracket", bracket_rows),
        "head_to_head": _insert_rows(client, "website_head_to_head", h2h_rows),
        "head_to_head_matches": _insert_rows(client, "website_head_to_head_matches", h2h_match_rows),
        "match_results": _insert_rows(client, "website_match_results", match_results_rows),
        "match_results_rows": _insert_rows(client, "website_match_results_rows", parsed_match_results_rows),
        "player_stats": _insert_rows(client, "website_player_stats", player_stats_rows),
        "player_stats_records": _insert_rows(client, "website_player_stats_records", player_stats_records_rows),
        "who_is_playing": _insert_rows(client, "website_who_is_playing", who_is_playing_rows),
        "who_is_playing_players": _insert_rows(client, "website_who_is_playing_players", who_is_playing_players_rows),
    }

    return {
        "snapshot_ts_utc": snapshot_ts,
        "ingest_run_id": ingest_run_id,
        "start_year": start_year,
        "end_year": end_year,
        "truncate": truncate,
        "truncate_schedule": truncate_schedule,
        "responses_root": str(responses_root),
        "written": written,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="ATP website ingestion for BigQuery (file-driven)")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--years", type=int, default=5)
    parser.add_argument("--truncate", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--truncate-schedule", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--sleep", type=float, default=0.0)
    args = parser.parse_args()

    now_year = datetime.now(timezone.utc).year
    if args.mode == "daily":
        start_year = args.start_year or now_year
        end_year = args.end_year or now_year
        truncate = False
        truncate_schedule = bool(args.truncate_schedule)
    else:
        end_year = args.end_year or now_year
        start_year = args.start_year if args.start_year is not None else max(1990, end_year - args.years + 1)
        truncate = bool(args.truncate)
        truncate_schedule = bool(args.truncate_schedule)

    result = run_ingest(start_year=start_year, end_year=end_year, truncate=truncate, truncate_schedule=truncate_schedule, sleep_seconds=args.sleep)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
