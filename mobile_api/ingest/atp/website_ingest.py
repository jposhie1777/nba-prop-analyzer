#/workspaces/nba-prop-analyzer/mobile_api/ingest/atp/website_ingest.py
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

import requests
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

# Static end-date lookup keyed by (tournament_id, year).
# Used for round-only archive pages that contain no date information in HTML.
# Dates represent the final day of each tournament.
_STATIC_TOURNAMENT_END_DATES: Dict[Tuple[str, int], str] = {
    # Australian Open (580)
    ("580", 2020): "2020-02-02", ("580", 2021): "2021-02-21", ("580", 2022): "2022-01-30",
    ("580", 2023): "2023-01-29", ("580", 2024): "2024-01-28", ("580", 2025): "2025-01-26",
    # Roland Garros (520)
    ("520", 2020): "2020-10-11", ("520", 2021): "2021-06-13", ("520", 2022): "2022-06-05",
    ("520", 2023): "2023-06-11", ("520", 2024): "2024-06-09", ("520", 2025): "2025-06-08",
    # Wimbledon (540)
    ("540", 2021): "2021-07-11", ("540", 2022): "2022-07-10",
    ("540", 2023): "2023-07-16", ("540", 2024): "2024-07-14", ("540", 2025): "2025-07-13",
    # US Open (560)
    ("560", 2020): "2020-09-13", ("560", 2021): "2021-09-12", ("560", 2022): "2022-09-11",
    ("560", 2023): "2023-09-10", ("560", 2024): "2024-09-08", ("560", 2025): "2025-09-07",
    # Indian Wells (404)
    ("404", 2021): "2021-10-17", ("404", 2022): "2022-03-20", ("404", 2023): "2023-03-19",
    ("404", 2024): "2024-03-17", ("404", 2025): "2025-03-16",
    # Miami (403)
    ("403", 2021): "2021-04-04", ("403", 2022): "2022-04-03", ("403", 2023): "2023-04-02",
    ("403", 2024): "2024-03-31", ("403", 2025): "2025-03-30",
    # Monte Carlo (410)
    ("410", 2021): "2021-04-18", ("410", 2022): "2022-04-17", ("410", 2023): "2023-04-16",
    ("410", 2024): "2024-04-14", ("410", 2025): "2025-04-13",
    # Madrid (1536)
    ("1536", 2021): "2021-05-09", ("1536", 2022): "2022-05-08", ("1536", 2023): "2023-05-07",
    ("1536", 2024): "2024-05-05", ("1536", 2025): "2025-05-04",
    # Rome (416)
    ("416", 2020): "2020-09-21", ("416", 2021): "2021-05-16", ("416", 2022): "2022-05-15",
    ("416", 2023): "2023-05-21", ("416", 2024): "2024-05-19", ("416", 2025): "2025-05-18",
    # Barcelona (425)
    ("425", 2021): "2021-04-25", ("425", 2022): "2022-04-24", ("425", 2023): "2023-04-23",
    ("425", 2024): "2024-04-21", ("425", 2025): "2025-04-20",
    # Hamburg (414)
    ("414", 2020): "2020-09-27", ("414", 2021): "2021-07-25", ("414", 2022): "2022-07-24",
    ("414", 2023): "2023-07-23", ("414", 2024): "2024-07-21", ("414", 2025): "2025-07-20",
    # Washington (418)
    ("418", 2021): "2021-08-08", ("418", 2022): "2022-08-07", ("418", 2023): "2023-08-06",
    ("418", 2024): "2024-08-04", ("418", 2025): "2025-08-03",
    # Montreal/Toronto (421)
    ("421", 2021): "2021-08-15", ("421", 2022): "2022-08-14", ("421", 2023): "2023-08-13",
    ("421", 2024): "2024-08-11", ("421", 2025): "2025-08-10",
    # Cincinnati (422)
    ("422", 2020): "2020-08-29", ("422", 2021): "2021-08-22", ("422", 2022): "2022-08-21",
    ("422", 2023): "2023-08-20", ("422", 2024): "2024-08-18", ("422", 2025): "2025-08-17",
    # Shanghai (5014)
    ("5014", 2023): "2023-10-15", ("5014", 2024): "2024-10-13", ("5014", 2025): "2025-10-12",
    # Paris Bercy (352)
    ("352", 2020): "2020-11-08", ("352", 2021): "2021-11-07", ("352", 2022): "2022-11-06",
    ("352", 2023): "2023-11-05", ("352", 2024): "2024-11-03", ("352", 2025): "2025-11-02",
    # Vienna (337)
    ("337", 2020): "2020-11-01", ("337", 2021): "2021-10-31", ("337", 2022): "2022-10-30",
    ("337", 2023): "2023-10-29", ("337", 2024): "2024-10-27", ("337", 2025): "2025-10-26",
    # Basel (328)
    ("328", 2021): "2021-10-31", ("328", 2022): "2022-10-30", ("328", 2023): "2023-10-29",
    ("328", 2024): "2024-10-27", ("328", 2025): "2025-10-26",
    # Nitto ATP Finals (605)
    ("605", 2020): "2020-11-22", ("605", 2021): "2021-11-21", ("605", 2022): "2022-11-20",
    ("605", 2023): "2023-11-19", ("605", 2024): "2024-11-17", ("605", 2025): "2025-11-16",
    # Dubai (495)
    ("495", 2020): "2020-02-29", ("495", 2021): "2021-03-06", ("495", 2022): "2022-02-26",
    ("495", 2023): "2023-03-04", ("495", 2024): "2024-03-02", ("495", 2025): "2025-03-01",
    # Acapulco (807)
    ("807", 2020): "2020-02-29", ("807", 2021): "2021-03-06", ("807", 2022): "2022-02-26",
    ("807", 2023): "2023-03-04", ("807", 2024): "2024-03-02", ("807", 2025): "2025-03-01",
    # Doha (451)
    ("451", 2020): "2020-01-11", ("451", 2021): "2021-03-13", ("451", 2022): "2022-02-19",
    ("451", 2023): "2023-02-18", ("451", 2024): "2024-02-17", ("451", 2025): "2025-02-22",
    # Rotterdam (407)
    ("407", 2020): "2020-02-16", ("407", 2021): "2021-03-07", ("407", 2022): "2022-02-13",
    ("407", 2023): "2023-02-12", ("407", 2024): "2024-02-11", ("407", 2025): "2025-02-09",
    # Dallas (424)
    ("424", 2022): "2022-02-13", ("424", 2023): "2023-02-12", ("424", 2024): "2024-02-11",
    ("424", 2025): "2025-02-09",
    # Montpellier (375)
    ("375", 2020): "2020-02-09", ("375", 2021): "2021-02-07", ("375", 2022): "2022-02-06",
    ("375", 2023): "2023-02-05", ("375", 2024): "2024-02-04", ("375", 2025): "2025-02-02",
    # Buenos Aires (506)
    ("506", 2020): "2020-02-16", ("506", 2021): "2021-04-11", ("506", 2022): "2022-02-13",
    ("506", 2023): "2023-02-19", ("506", 2024): "2024-02-18", ("506", 2025): "2025-02-16",
    # Rio (6932)
    ("6932", 2020): "2020-02-23", ("6932", 2021): "2021-04-18", ("6932", 2022): "2022-02-20",
    ("6932", 2023): "2023-02-26", ("6932", 2024): "2024-02-25", ("6932", 2025): "2025-02-23",
    # Delray Beach (499)
    ("499", 2020): "2020-02-23", ("499", 2021): "2021-04-11", ("499", 2022): "2022-02-20",
    ("499", 2023): "2023-02-19", ("499", 2024): "2024-02-18", ("499", 2025): "2025-02-16",
    # Halle (500)
    ("500", 2021): "2021-06-20", ("500", 2022): "2022-06-19", ("500", 2023): "2023-06-18",
    ("500", 2024): "2024-06-23", ("500", 2025): "2025-06-22",
    # Stuttgart (321)
    ("321", 2021): "2021-06-13", ("321", 2022): "2022-06-12", ("321", 2023): "2023-06-11",
    ("321", 2024): "2024-06-16", ("321", 2025): "2025-06-15",
    # London Queens (311)
    ("311", 2021): "2021-06-20", ("311", 2022): "2022-06-19", ("311", 2023): "2023-06-18",
    ("311", 2024): "2024-06-23", ("311", 2025): "2025-06-22",
    # Eastbourne (741)
    ("741", 2021): "2021-06-26", ("741", 2022): "2022-06-25", ("741", 2023): "2023-06-24",
    ("741", 2024): "2024-06-29", ("741", 2025): "2025-06-28",
    # Mallorca (8994)
    ("8994", 2021): "2021-06-26", ("8994", 2022): "2022-06-25", ("8994", 2023): "2023-06-24",
    ("8994", 2024): "2024-06-29", ("8994", 2025): "2025-06-28",
    # S-Hertogenbosch (440)
    ("440", 2021): "2021-06-13", ("440", 2022): "2022-06-12", ("440", 2023): "2023-06-11",
    ("440", 2024): "2024-06-16", ("440", 2025): "2025-06-15",
    # Bastad (316)
    ("316", 2021): "2021-07-25", ("316", 2022): "2022-07-24", ("316", 2023): "2023-07-23",
    ("316", 2024): "2024-07-21", ("316", 2025): "2025-07-20",
    # Gstaad (314)
    ("314", 2021): "2021-07-25", ("314", 2022): "2022-07-24", ("314", 2023): "2023-07-23",
    ("314", 2024): "2024-07-21", ("314", 2025): "2025-07-20",
    # Umag (439)
    ("439", 2021): "2021-07-25", ("439", 2022): "2022-07-24", ("439", 2023): "2023-07-23",
    ("439", 2024): "2024-07-21", ("439", 2025): "2025-07-20",
    # Kitzbuhel (319)
    ("319", 2021): "2021-08-01", ("319", 2022): "2022-07-31", ("319", 2023): "2023-07-30",
    ("319", 2024): "2024-07-28", ("319", 2025): "2025-07-27",
    # Los Cabos (7480)
    ("7480", 2021): "2021-08-01", ("7480", 2022): "2022-07-31", ("7480", 2023): "2023-07-30",
    ("7480", 2024): "2024-07-28", ("7480", 2025): "2025-08-03",
    # Winston-Salem (6242)
    ("6242", 2021): "2021-08-28", ("6242", 2022): "2022-08-27", ("6242", 2023): "2023-08-26",
    ("6242", 2024): "2024-08-24", ("6242", 2025): "2025-08-23",
    # Stockholm (429)
    ("429", 2021): "2021-11-14", ("429", 2022): "2022-10-23", ("429", 2023): "2023-10-22",
    ("429", 2024): "2024-10-20", ("429", 2025): "2025-10-19",
    # Moscow/St Pete (568)
    ("568", 2021): "2021-10-24",
    # Metz (341)
    ("341", 2021): "2021-09-26", ("341", 2022): "2022-09-25", ("341", 2023): "2023-09-24",
    ("341", 2024): "2024-09-22", ("341", 2025): "2025-09-21",
    # Tokyo (329)
    ("329", 2021): "2021-10-10", ("329", 2022): "2022-10-09", ("329", 2023): "2023-10-08",
    ("329", 2024): "2024-10-06", ("329", 2025): "2025-10-05",
    # Beijing (747)
    ("747", 2023): "2023-10-08", ("747", 2024): "2024-10-06", ("747", 2025): "2025-10-05",
    # Chengdu (7581)
    ("7581", 2023): "2023-10-01", ("7581", 2024): "2024-09-29", ("7581", 2025): "2025-09-28",
    # Hangzhou (4713)
    ("4713", 2024): "2024-10-06", ("4713", 2025): "2025-10-05",
    # Geneva (322)
    ("322", 2021): "2021-05-22", ("322", 2022): "2022-05-21", ("322", 2023): "2023-05-20",
    ("322", 2024): "2024-05-25", ("322", 2025): "2025-05-24",
    # Lyon (496)
    ("496", 2021): "2021-05-23", ("496", 2022): "2022-05-22", ("496", 2023): "2023-05-28",
    ("496", 2024): "2024-05-26", ("496", 2025): "2025-05-25",
    # Munich (308)
    ("308", 2021): "2021-05-02", ("308", 2022): "2022-05-01", ("308", 2023): "2023-04-30",
    ("308", 2024): "2024-04-28", ("308", 2025): "2025-04-27",
    # Marrakech (360)
    ("360", 2021): "2021-04-11", ("360", 2022): "2022-04-10", ("360", 2023): "2023-04-09",
    ("360", 2024): "2024-04-14", ("360", 2025): "2025-04-13",
    # Estoril (7290)
    ("7290", 2021): "2021-05-02", ("7290", 2022): "2022-05-01", ("7290", 2023): "2023-04-30",
    ("7290", 2024): "2024-04-28", ("7290", 2025): "2025-04-27",
    # Houston (717)
    ("717", 2021): "2021-04-11", ("717", 2022): "2022-04-10", ("717", 2023): "2023-04-09",
    ("717", 2024): "2024-04-14", ("717", 2025): "2025-04-13",
    # Newport (315)
    ("315", 2021): "2021-07-18", ("315", 2022): "2022-07-17", ("315", 2023): "2023-07-16",
    ("315", 2024): "2024-07-21", ("315", 2025): "2025-07-20",
    # Auckland (301)
    ("301", 2020): "2020-01-11", ("301", 2022): "2022-01-08", ("301", 2023): "2023-01-07",
    ("301", 2024): "2024-01-13", ("301", 2025): "2025-01-11",
    # Adelaide (8998)
    ("8998", 2020): "2020-01-11", ("8998", 2021): "2021-02-13", ("8998", 2022): "2022-01-08",
    ("8998", 2023): "2023-01-07", ("8998", 2024): "2024-01-12", ("8998", 2025): "2025-01-11",
    # Santiago (8996)
    ("8996", 2020): "2020-03-01", ("8996", 2021): "2021-04-04", ("8996", 2022): "2022-03-06",
    ("8996", 2023): "2023-03-05", ("8996", 2024): "2024-03-03", ("8996", 2025): "2025-03-02",
    # Almaty (9410)
    ("9410", 2020): "2020-11-01", ("9410", 2021): "2021-10-10", ("9410", 2022): "2022-10-02",
    ("9410", 2023): "2023-10-01", ("9410", 2024): "2024-09-29", ("9410", 2025): "2025-09-28",
    # ATP Cup (8888)
    ("8888", 2020): "2020-01-12", ("8888", 2021): "2021-02-07", ("8888", 2022): "2022-01-09",
    # Brussels (7485)
    ("7485", 2021): "2021-11-21", ("7485", 2022): "2022-10-23", ("7485", 2023): "2023-10-22",
    ("7485", 2024): "2024-10-20", ("7485", 2025): "2025-10-19",
    # Brisbrane (339)
    ("339", 2020): "2020-01-05", ("339", 2022): "2022-01-08", ("339", 2023): "2023-01-07",
    ("339", 2024): "2024-01-07", ("339", 2025): "2025-01-05",
    # Hong Kong (336)
    ("336", 2024): "2024-01-13", ("336", 2025): "2025-01-11",
    # Next Gen ATP Finals (7696)
    ("7696", 2021): "2021-12-04", ("7696", 2022): "2022-11-12", ("7696", 2023): "2023-11-11",
    ("7696", 2024): "2024-11-09", ("7696", 2025): "2025-11-08",
    # Pune (891)
    ("891", 2020): "2020-01-05", ("891", 2022): "2022-01-08", ("891", 2023): "2023-01-07",
    # Cordoba (9158)
    ("9158", 2020): "2020-02-09", ("9158", 2021): "2021-04-04", ("9158", 2022): "2022-02-06",
    ("9158", 2023): "2023-02-05", ("9158", 2024): "2024-02-04", ("9158", 2025): "2025-02-02",
    # Zhuhai (9164)
    ("9164", 2023): "2023-10-01",
    # Paris Olympics (96)
    ("96", 2020): "2020-08-02", ("96", 2021): "2021-08-01", ("96", 2024): "2024-08-04",
    # San Diego (9569)
    ("9569", 2022): "2022-10-09",
    # Belgrade (5053)
    ("5053", 2022): "2022-04-24",
    # Belgrade (4787)
    ("4787", 2024): "2024-04-21",
    # Gijon (2807)
    ("2807", 2022): "2022-10-16",
    # Tel Aviv (4138)
    ("4138", 2022): "2022-10-02",
    # Sofia (7434)
    ("7434", 2022): "2022-02-06", ("7434", 2023): "2023-02-05",
    # Bucharest (4462)
    ("4462", 2024): "2024-04-21",
    # Athens (5100)
    ("5100", 2025): "2025-11-09",
    # Metz/Lyon (7694)
    ("7694", 2021): "2021-05-23", ("7694", 2022): "2022-05-22", ("7694", 2023): "2023-05-28",
    ("7694", 2024): "2024-05-26", ("7694", 2025): "2025-05-25",
}

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


def _extract_player_id_from_profile_url(profile_url: Optional[str]) -> Optional[str]:
    if not profile_url:
        return None
    m = re.search(r"/en/players/[^/]+/([^/?#]+)/", profile_url, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).upper()


def _build_h2h_pairs_from_schedule_rows(rows: Sequence[Dict[str, Any]]) -> List[Tuple[str, str]]:
    seen: Set[Tuple[str, str]] = set()
    ordered_pairs: List[Tuple[str, str]] = []
    for row in rows:
        p1 = _extract_player_id_from_profile_url(row.get("player_1_profile_url"))
        p2 = _extract_player_id_from_profile_url(row.get("player_2_profile_url"))
        if not p1 or not p2 or p1 == p2:
            continue
        key = tuple(sorted((p1, p2)))
        if key in seen:
            continue
        seen.add(key)
        ordered_pairs.append((p1, p2))
    return ordered_pairs


def _fetch_json_url(url: str, timeout: int = 20) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(url, timeout=timeout, headers={"User-Agent": "nba-prop-analyzer-atp-website-ingest/1.0"})
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


def _fetch_html_url(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch a URL as raw HTML text, returning None on any failure."""
    try:
        response = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        response.raise_for_status()
        return response.text
    except Exception:
        return None


def _extract_player_ids_from_html(html: str) -> Set[str]:
    """Extract all unique ATP player IDs from /en/players/.../ID/overview URLs in any HTML."""
    ids: Set[str] = set()
    for m in re.finditer(
        r"/en/players/(?!atp-head-2-head)[^/]+/([A-Za-z0-9]{4})/overview",
        html,
        flags=re.IGNORECASE,
    ):
        ids.add(m.group(1).upper())
    return ids



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




def _extract_daily_schedule_time_fields(payload_html: Optional[str]) -> Tuple[List[str], List[str], List[Dict[str, Optional[str]]]]:
    if not payload_html:
        return [], [], []

    start_times: List[str] = []
    not_before_times: List[str] = []
    schedule_time_items: List[Dict[str, Optional[str]]] = []

    # Match the entire opening <div class="schedule" ...> tag so we can search
    # each attribute independently — the live-fetched HTML may have attributes in
    # a different order than the captured file.
    tag_pattern = re.compile(r'<div class="schedule"([^>]*)>', flags=re.IGNORECASE)

    for tag_m in tag_pattern.finditer(payload_html):
        attrs = tag_m.group(1)
        datetime_m = re.search(r'data-datetime="([^"]*)"', attrs, re.IGNORECASE)
        displaytime_m = re.search(r'data-displaytime="([^"]*)"', attrs, re.IGNORECASE)
        if not displaytime_m:
            continue

        data_datetime = (datetime_m.group(1) if datetime_m else '').strip() or None
        display_time = displaytime_m.group(1).strip()
        if not display_time:
            continue

        lower = display_time.lower()
        time_type = None
        if lower.startswith('starts at'):
            time_type = 'starts_at'
            start_times.append(display_time)
        elif lower.startswith('not before'):
            time_type = 'not_before'
            not_before_times.append(display_time)

        schedule_time_items.append(
            {
                'display_time': display_time,
                'data_datetime': data_datetime,
                'time_type': time_type,
            }
        )

    return start_times[:500], not_before_times[:500], schedule_time_items[:500]

def _parse_tournament_end_date(context_html: str, year: int) -> Optional[str]:
    """Extract the tournament end date from the HTML context surrounding an archive link."""
    months = {m: i + 1 for i, m in enumerate([
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ])}
    # Same-month range: "7 - 13 February, 2022"
    m1 = re.search(
        r'\d+\s*[-–]\s*(\d+)\s+(January|February|March|April|May|June|July|August'
        r'|September|October|November|December),?\s*(\d{4})',
        context_html, re.IGNORECASE,
    )
    if m1:
        try:
            from datetime import date as date_type
            return date_type(int(m1.group(3)), months[m1.group(2).lower()], int(m1.group(1))).isoformat()
        except Exception:
            pass
    # Cross-month range: "January 18 - February 1, 2026"
    m2 = re.search(
        r'(?:January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+\d+\s*[-–]\s*((?:January|February|March|April|May|June|July|August'
        r'|September|October|November|December)\s+\d+),?\s*(\d{4})',
        context_html, re.IGNORECASE,
    )
    if m2:
        try:
            return datetime.strptime(f"{m2.group(1)}, {m2.group(2)}", "%B %d, %Y").date().isoformat()
        except Exception:
            pass
    return None

def _fetch_tournament_end_dates(client: bigquery.Client, dataset: str, start_year: int, end_year: int) -> Dict[str, str]:
    """Build a tournament_id -> end_date_iso map from two sources:
    1. website_tournaments BQ table (already ingested, any year)
    2. ATP calendar JSON API for each backfill year (live fetch)
    """
    months = {m: i + 1 for i, m in enumerate([
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
        "jan", "feb", "mar", "apr", "may", "jun",
        "jul", "aug", "sep", "oct", "nov", "dec",
    ])}

    def _parse_end_date(fmt: str) -> Optional[str]:
        if not fmt:
            return None
        # Same-month: "7 - 13 February, 2022" or "2 - 11 January, 2026"
        m1 = re.search(
            r'\d+\s*[-–]\s*(\d+)\s+'
            r'(January|February|March|April|May|June|July|August|September|October|November|December'
            r'|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
            r',?\s*(\d{4})',
            fmt, re.IGNORECASE,
        )
        if m1:
            try:
                from datetime import date as dt
                mon = months[m1.group(2).lower()]
                return dt(int(m1.group(3)), mon, int(m1.group(1))).isoformat()
            except Exception:
                pass
        # Cross-month: "18 January - 1 February, 2026" or "30 Jun - 13 Jul, 2025"
        m2 = re.search(
            r'\d+\s+'
            r'(?:January|February|March|April|May|June|July|August|September|October|November|December'
            r'|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
            r'\s*[-–]\s*'
            r'(\d+)\s+'
            r'(January|February|March|April|May|June|July|August|September|October|November|December'
            r'|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
            r',?\s*(\d{4})',
            fmt, re.IGNORECASE,
        )
        if m2:
            try:
                from datetime import date as dt
                mon = months[m2.group(2).lower()]
                return dt(int(m2.group(3)), mon, int(m2.group(1))).isoformat()
            except Exception:
                pass
        return None

    result: Dict[str, str] = {}

    # ------------------------------------------------------------------ #
    # Source 1: BQ website_tournaments (fast, already ingested)           #
    # ------------------------------------------------------------------ #
    try:
        rows = list(client.query(
            f"SELECT tournament_id, formatted_date "
            f"FROM `{dataset}.website_tournaments` "
            f"WHERE formatted_date IS NOT NULL"
        ).result())
        for row in rows:
            d = _parse_end_date(row["formatted_date"] or "")
            if d:
                result[str(row["tournament_id"])] = d
    except Exception:
        pass

    # ------------------------------------------------------------------ #
    # Source 2: ATP calendar JSON API — one call per backfill year.       #
    # Fills in tournament IDs not covered by the BQ table (prior years). #
    # ------------------------------------------------------------------ #
    for year in range(start_year, end_year + 1):
        # Try two known ATP calendar endpoint patterns
        data: Optional[Dict[str, Any]] = None
        for cal_url in [
            f"https://www.atptour.com/en/-/www/calendar/tournaments/{year}",
            f"https://www.atptour.com/en/-/www/tournaments/dates/{year}",
        ]:
            data = _fetch_json_url(cal_url)
            if isinstance(data, dict):
                break

        if not isinstance(data, dict):
            print(f"[backfill] WARNING: no calendar data for year={year}", flush=True)
            continue

        for month in data.get("TournamentDates", []) or []:
            for t in month.get("Tournaments", []) or []:
                tid = str(t.get("Id") or "")
                if not tid or tid in result:
                    continue
                fmt = t.get("FormattedDate") or ""
                d = _parse_end_date(fmt)
                if d:
                    result[tid] = d

        print(f"[backfill] calendar year={year}: {len(result)} end dates total", flush=True)

    print(f"[backfill] tournament end dates resolved: {len(result)}", flush=True)
    return result

def _build_tournament_end_dates_from_captures(
    historical: List[Tuple[str, str, Path, int]],
) -> Dict[Tuple[str, int], str]:
    """
    Build a (tournament_id, year) -> end_date_iso map by scanning each
    historical capture file for dated tournament-day section headers.
    Takes the latest date found as the tournament end date for that year.
    Falls back to the static lookup table for round-only tournaments.
    """
    months = {m: i + 1 for i, m in enumerate([
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ])}
    date_re = re.compile(
        r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,\s+(\d+)\s+'
        r'(January|February|March|April|May|June|July|August'
        r'|September|October|November|December)'
        r',?\s+(\d{4})',
        re.IGNORECASE,
    )
    result: Dict[Tuple[str, int], str] = {}

    for slug, tid, file_path, year in historical:
        if (tid, year) in result:
            continue
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
            all_dates = []
            for m in date_re.finditer(text):
                try:
                    from datetime import date as dt
                    d = dt(int(m.group(3)), months[m.group(2).lower()], int(m.group(1)))
                    all_dates.append(d)
                except Exception:
                    pass
            if all_dates:
                result[(tid, year)] = max(all_dates).isoformat()
        except Exception:
            pass

    # Merge static lookup for round-only tournaments that have no dates in HTML
    for (tid, year), end_date_iso in _STATIC_TOURNAMENT_END_DATES.items():
        if (tid, year) not in result:
            result[(tid, year)] = end_date_iso

    print(f"[ingest] year-specific end dates from captures: {len(result)}", flush=True)
    return result

def _load_historical_captures(
    output_root: Path,
    start_year: int,
    end_year: int,
) -> List[Tuple[str, str, Path, int]]:
    """
    Scan the historical capture directory and return
    (slug, tid, path, year) for every saved capture file.
    Filename format: {slug}_{tid}
    """
    results: List[Tuple[str, str, Path, int]] = []
    for year in range(start_year, end_year + 1):
        year_dir = output_root / str(year)
        if not year_dir.exists():
            continue
        for path in sorted(year_dir.iterdir()):
            if not path.is_file():
                continue
            parts = path.name.rsplit("_", 1)
            if len(parts) != 2:
                continue
            slug, tid = parts[0], parts[1]
            results.append((slug, tid, path, year))
    return results


def _fetch_tournament_results_urls_for_year(year: int) -> List[Tuple[str, str, str]]:
    """
    Scrape the ATP results-archive page for a given year and return
    (slug, tournament_id, results_url) for every tournament listed.
    """
    url = f"https://www.atptour.com/en/scores/results-archive?year={year}"
    html = _fetch_html_url(url)
    if not html:
        print(f"[backfill] WARNING: no archive page for year={year}", flush=True)
        return []

    out: List[Tuple[str, str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for m in re.finditer(
        r'href="(/en/scores/archive/([^/]+)/([^/]+)/' + str(year) + r'/results)"',
        html, flags=re.IGNORECASE,
    ):
        path, slug, tid = m.group(1), m.group(2), m.group(3)
        if (slug, tid) in seen:
            continue
        seen.add((slug, tid))
        out.append((slug, tid, f"https://www.atptour.com{path}"))

    print(f"[backfill] year={year}: {len(out)} tournaments found", flush=True)
    return out


def run_ingest(start_year: int, end_year: int, truncate: bool, truncate_schedule: bool, sleep_seconds: float) -> Dict[str, Any]:
    del sleep_seconds

    snapshot_ts = utc_now_iso()
    ingest_run_id = str(uuid.uuid4())
    client = _bq_client()

    # ------------------------------------------------------------------ #
    # Load tournament end dates BEFORE any truncation — the truncate step #
    # wipes website_tournaments, so we must read it first.                #
    # ------------------------------------------------------------------ #
    tournament_end_dates: Dict[str, str] = (
        _fetch_tournament_end_dates(client, _dataset(), start_year, end_year)
        if (start_year and end_year)
        else {}
    )

    historical_root_pre = Path(os.getenv("ATP_HISTORICAL_DIR", "website_responses/atp/historical"))
    historical_pre = (
        _load_historical_captures(historical_root_pre, start_year, end_year)
        if (start_year and end_year)
        else []
    )
    tournament_end_dates_by_year: Dict[Tuple[str, int], str] = (
        _build_tournament_end_dates_from_captures(historical_pre)
    )
    for tid, end_date_iso in tournament_end_dates.items():
        try:
            from datetime import date as dt
            candidate = dt.fromisoformat(end_date_iso)
            if (tid, candidate.year) not in tournament_end_dates_by_year:
                tournament_end_dates_by_year[(tid, candidate.year)] = end_date_iso
        except Exception:
            pass

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
        _truncate_table(client, "website_match_results")
        _truncate_table(client, "website_match_results_rows")
        _truncate_table(client, "website_head_to_head")
        _truncate_table(client, "website_head_to_head_matches")

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
    parsed_match_results_rows: List[Dict[str, Any]] = []
    player_stats_rows: List[Dict[str, Any]] = []
    player_stats_records_rows: List[Dict[str, Any]] = []
    who_is_playing_rows: List[Dict[str, Any]] = []
    who_is_playing_players_rows: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------ #
    # Tournament calendar                                                  #
    # ------------------------------------------------------------------ #
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

    # ------------------------------------------------------------------ #
    # Daily schedule                                                       #
    # ------------------------------------------------------------------ #
    daily_schedule_capture = captures["daily_schedule"]
    _ds_slug, _ds_tid = _extract_slug_and_tournament_id(daily_schedule_capture.get("request_url"))
    if _ds_slug and _ds_tid:
        _live_schedule_url = f"https://www.atptour.com/en/scores/current/{_ds_slug}/{_ds_tid}/daily-schedule"
        _live_schedule_html = _fetch_html_url(_live_schedule_url)
        if _live_schedule_html:
            daily_schedule_capture = {
                **daily_schedule_capture,
                "payload_text": _live_schedule_html,
                "request_url": _live_schedule_url,
            }

    daily_text_chunks, daily_links = _flatten_html_payload(daily_schedule_capture.get("payload_text"))
    daily_start_times, daily_not_before_times, daily_time_items = _extract_daily_schedule_time_fields(
        daily_schedule_capture.get("payload_text")
    )
    daily_schedule_rows.append(
        {
            "snapshot_ts_utc": snapshot_ts,
            "ingest_run_id": ingest_run_id,
            "url": daily_schedule_capture.get("request_url"),
            "payload_html": daily_schedule_capture.get("payload_text"),
            "flattened_text_chunks": daily_text_chunks,
            "flattened_links": daily_links,
            "start_time_labels": daily_start_times,
            "not_before_labels": daily_not_before_times,
            "schedule_time_items": daily_time_items,
        }
    )

    sched_slug, sched_tid = _extract_slug_and_tournament_id(daily_schedule_capture.get("request_url"))
    all_schedule_rows: List[Dict[str, Any]] = []
    if sched_slug and sched_tid and daily_schedule_capture.get("payload_text"):
        all_schedule_rows = [
            row.to_dict()
            for row in normalize_match_schedule_html(
                sched_slug,
                sched_tid,
                daily_schedule_capture["payload_text"],
                snapshot_ts_utc=snapshot_ts,
                include_past=True,
            )
        ]
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

    # ------------------------------------------------------------------ #
    # Draws / bracket                                                      #
    # ------------------------------------------------------------------ #
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

    # ------------------------------------------------------------------ #
    # Head-to-head                                                         #
    # ------------------------------------------------------------------ #
    h2h_capture = captures["head_to_head"]
    h2h_pairs = _build_h2h_pairs_from_schedule_rows(all_schedule_rows)

    fallback_left_id, fallback_right_id = _extract_h2h_ids(h2h_capture.get("request_url"))
    fallback_payload = h2h_capture.get("payload_json") if isinstance(h2h_capture.get("payload_json"), dict) else None
    if not h2h_pairs and fallback_left_id and fallback_right_id:
        h2h_pairs = [(fallback_left_id, fallback_right_id)]

    for left_id, right_id in h2h_pairs:
        h2h_url = f"https://www.atptour.com/en/-/www/h2h/{left_id.lower()}/{right_id.lower()}"
        payload_json = _fetch_json_url(h2h_url)
        if payload_json is None and fallback_payload and {left_id, right_id} == {fallback_left_id, fallback_right_id}:
            payload_json = fallback_payload

        h2h_rows.append(
            {
                "snapshot_ts_utc": snapshot_ts,
                "ingest_run_id": ingest_run_id,
                "url": h2h_url,
                "payload_json": _safe_json_str(payload_json),
            }
        )
        if isinstance(payload_json, dict):
            h2h_match_rows.extend(
                [
                    row.to_dict()
                    for row in normalize_head_to_head(left_id, right_id, payload_json, snapshot_ts_utc=snapshot_ts)
                ]
            )

    # ------------------------------------------------------------------ #
    # Match results — current tournament (live fetch)                     #
    # ------------------------------------------------------------------ #
    results_capture = captures["match_results"]
    result_slug, result_tid = _extract_slug_and_tournament_id(results_capture.get("request_url"))
    if result_slug and result_tid:
        _live_results_url = f"https://www.atptour.com/en/scores/current/{result_slug}/{result_tid}/results"
        _live_results_html = _fetch_html_url(_live_results_url)
        if _live_results_html:
            results_capture = {
                **results_capture,
                "payload_text": _live_results_html,
                "request_url": _live_results_url,
            }
    if result_slug and result_tid and results_capture.get("payload_text"):
        parsed_match_results_rows.extend(
            row.to_dict()
            for row in normalize_match_results_html(
                result_slug,
                result_tid,
                results_capture["payload_text"],
                snapshot_ts_utc=snapshot_ts,
            )
        )

    # ------------------------------------------------------------------ #
    # Match results — backfill across all requested years                 #
    # ------------------------------------------------------------------ #
    if start_year and end_year:
        already_fetched: Set[Tuple[str, str, int]] = set()

        def _process_match_html(slug: str, tid: str, html: str, year: Optional[int] = None) -> None:
            end_date = None
            if year is not None:
                end_date_iso = tournament_end_dates_by_year.get((tid, year))
                if end_date_iso:
                    try:
                        from datetime import date as date_type
                        end_date = date_type.fromisoformat(end_date_iso)
                    except Exception:
                        pass
            parsed_match_results_rows.extend(
                row.to_dict()
                for row in normalize_match_results_html(
                    slug, tid, html,
                    snapshot_ts_utc=snapshot_ts,
                    tournament_end_date=end_date,
                )
            )

        # Path 1: historical capture files (Camoufox-captured, always preferred)
        historical_root = Path(os.getenv("ATP_HISTORICAL_DIR", "website_responses/atp/historical"))
        historical = _load_historical_captures(historical_root, start_year, end_year)
        print(f"[ingest] historical capture files found: {len(historical)}", flush=True)

        for slug, tid, file_path, year in historical:
            if (slug, tid, year) in already_fetched:
                continue
            already_fetched.add((slug, tid, year))
            try:
                capture = _load_capture_file(file_path)
                html = capture.get("payload_text")
            except Exception as exc:
                print(f"[ingest] WARNING: failed to load {file_path}: {exc}", flush=True)
                html = None
            if html:
                _process_match_html(slug, tid, html, year)

        for year in range(start_year, end_year + 1):
            for past_slug, past_tid, past_url in _fetch_tournament_results_urls_for_year(year):
                if (past_slug, past_tid, year) in already_fetched:
                    continue
                already_fetched.add((past_slug, past_tid, year))
                past_html = _fetch_html_url(past_url)
                if past_html:
                    _process_match_html(past_slug, past_tid, past_html, year)
    # ------------------------------------------------------------------ #
    # Player IDs — harvested from all sources                             #
    # ------------------------------------------------------------------ #
    player_ids: Set[str] = set()
    for row in all_schedule_rows:
        p1 = _extract_player_id_from_profile_url(row.get("player_1_profile_url"))
        p2 = _extract_player_id_from_profile_url(row.get("player_2_profile_url"))
        if p1:
            player_ids.add(p1)
        if p2:
            player_ids.add(p2)

    draws_html = draws_capture.get("payload_text") or ""
    if draws_html:
        player_ids |= _extract_player_ids_from_html(draws_html)

    results_html_text = results_capture.get("payload_text") or ""
    if results_html_text:
        player_ids |= _extract_player_ids_from_html(results_html_text)

    who_payload_for_ids = captures["who_is_playing"].get("payload_json")
    if isinstance(who_payload_for_ids, dict):
        for player in who_payload_for_ids.get("PlayersList", []) or []:
            pid = _extract_player_id_from_profile_url(player.get("ProfileUrl"))
            if pid:
                player_ids.add(pid)

    # ------------------------------------------------------------------ #
    # Player stats                                                         #
    # ------------------------------------------------------------------ #
    default_stats_captures = {
        "all": captures["player_stats_all"],
        "clay": captures["player_stats_clay"],
        "grass": captures["player_stats_grass"],
        "hard": captures["player_stats_hard"],
    }
    surface_path = {"all": "all", "clay": "Clay", "grass": "Grass", "hard": "Hard"}

    if not player_ids:
        fallback_payload = default_stats_captures["all"].get("payload_json")
        fallback_player_id = None
        if isinstance(fallback_payload, dict):
            fallback_player_id = (fallback_payload.get("Stats") or {}).get("PlayerId")
        if fallback_player_id:
            player_ids.add(str(fallback_player_id).upper())

    for player_id in sorted(player_ids):
        for court_type in ["all", "clay", "grass", "hard"]:
            url = f"https://www.atptour.com/en/-/www/stats/{player_id.lower()}/all/{surface_path[court_type]}?v=1"
            payload_json = _fetch_json_url(url)

            default_capture = default_stats_captures[court_type]
            default_payload = default_capture.get("payload_json") if isinstance(default_capture.get("payload_json"), dict) else None
            if payload_json is None and isinstance(default_payload, dict):
                default_player_id = ((default_payload.get("Stats") or {}).get("PlayerId") or "").upper()
                if default_player_id == player_id:
                    payload_json = default_payload
                    url = default_capture.get("request_url") or url

            player_stats_rows.append(
                {
                    "snapshot_ts_utc": snapshot_ts,
                    "ingest_run_id": ingest_run_id,
                    "court_type": court_type,
                    "url": url,
                    "payload_json": _safe_json_str(payload_json),
                }
            )
            if isinstance(payload_json, dict):
                stats_block = payload_json.get("Stats") or {}
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

    # ------------------------------------------------------------------ #
    # Who is playing                                                       #
    # ------------------------------------------------------------------ #
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

    # ------------------------------------------------------------------ #
    # Write to BigQuery                                                    #
    # ------------------------------------------------------------------ #
    written = {
        "raw": _insert_rows(client, "website_raw_responses", raw_rows),
        "tournament_months": _insert_rows(client, "website_tournament_months", tournament_month_rows),
        "tournaments": _insert_rows(client, "website_tournaments", tournament_rows),
        "daily_schedule": _insert_rows(client, "website_daily_schedule", daily_schedule_rows),
        "upcoming_matches": _insert_rows(client, "website_upcoming_matches", upcoming_match_rows),
        "draws": _insert_rows(client, "website_draws", draws_rows),
        "tournament_bracket": _insert_rows(client, "website_tournament_bracket", bracket_rows),
        "head_to_head": _insert_rows(
            client,
            "website_head_to_head",
            [{**row, "ingest_run_id": ingest_run_id} for row in h2h_match_rows],
        ),
        "head_to_head_matches": _insert_rows(client, "website_head_to_head_matches", h2h_match_rows),
        "match_results": _insert_rows(
            client,
            "website_match_results",
            [{**row, "ingest_run_id": ingest_run_id} for row in parsed_match_results_rows],
        ),
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
