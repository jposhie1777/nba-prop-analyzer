from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
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


def _past_tournament_results_urls(tournament_dates_json: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """Return (slug, tournament_id, full_results_url) for each past event in tournament_dates JSON.

    URLs are built from the ScoresUrl field (archive URL) already present in the JSON.
    Team-event suffixes like 'country-results' are normalised to 'results'.
    """
    out: List[Tuple[str, str, str]] = []
    for month in tournament_dates_json.get("TournamentDates", []):
        for t in month.get("Tournaments", []):
            if not t.get("IsPastEvent"):
                continue
            scores_url = (t.get("ScoresUrl") or "").replace("country-results", "results")
            if not scores_url:
                continue
            # Extract slug and id: /en/scores/archive/{slug}/{id}/{year}/results
            m = re.search(r"/scores/(?:archive|current)/([^/]+)/([^/]+)/", scores_url)
            if not m:
                continue
            slug, tid = m.group(1), m.group(2)
            out.append((slug, tid, f"https://www.atptour.com{scores_url}"))
    return out


def _all_tournament_results_urls(tournament_dates_json: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """Return (slug, tournament_id, full_results_url) for all events in tournament_dates JSON."""
    out: List[Tuple[str, str, str]] = []
    for month in tournament_dates_json.get("TournamentDates", []):
        for t in month.get("Tournaments", []):
            scores_url = (t.get("ScoresUrl") or "").replace("country-results", "results")
            if not scores_url:
                continue
            m = re.search(r"/scores/(?:archive|current)/([^/]+)/([^/]+)/", scores_url)
            if not m:
                continue
            slug, tid = m.group(1), m.group(2)
            out.append((slug, tid, f"https://www.atptour.com{scores_url}"))
    deduped: Dict[str, Tuple[str, str, str]] = {}
    for slug, tid, url in out:
        deduped[url] = (slug, tid, url)
    return list(deduped.values())


def _year_from_results_url(url: str) -> Optional[int]:
    m = re.search(r"/(?:archive|current)/[^/]+/[^/]+/(\d{4})/", url)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _results_urls_for_year_range(
    tournament_dates_json: Dict[str, Any],
    start_year: int,
    end_year: int,
) -> List[Tuple[str, str, str]]:
    urls = _all_tournament_results_urls(tournament_dates_json)
    if not urls:
        return []
    filtered: List[Tuple[str, str, str]] = []
    for slug, tid, url in urls:
        year = _year_from_results_url(url)
        if year is None:
            filtered.append((slug, tid, url))
            continue
        if start_year <= year <= end_year:
            filtered.append((slug, tid, url))
    return filtered


def _extract_results_urls_from_archive_html(archive_html: str) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    for m in re.finditer(
        r'href="(/en/scores/archive/([^/]+)/([^/]+)/(\d{4})/(?:country-results|results))"',
        archive_html,
        flags=re.IGNORECASE,
    ):
        href = m.group(1).replace("country-results", "results")
        slug = m.group(2)
        tid = m.group(3)
        out.append((slug, tid, f"https://www.atptour.com{href}"))
    deduped: Dict[str, Tuple[str, str, str]] = {}
    for slug, tid, url in out:
        deduped[url] = (slug, tid, url)
    return list(deduped.values())


def _month_to_number(month_name: str) -> Optional[int]:
    lookup = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    return lookup.get(month_name.strip().lower())


def _parse_archive_event_date_range(date_text: str) -> Tuple[Optional[date], Optional[date]]:
    text = re.sub(r"\s+", " ", (date_text or "")).strip()
    if not text:
        return None, None

    patterns = [
        re.compile(
            r"^(\d{1,2})\s+([A-Za-z]+),\s*(\d{4})\s*-\s*(\d{1,2})\s+([A-Za-z]+),\s*(\d{4})$",
            flags=re.IGNORECASE,
        ),
        re.compile(
            r"^(\d{1,2})\s+([A-Za-z]+)\s*-\s*(\d{1,2})\s+([A-Za-z]+),\s*(\d{4})$",
            flags=re.IGNORECASE,
        ),
    ]

    m1 = patterns[0].match(text)
    if m1:
        d1, m1_name, y1, d2, m2_name, y2 = m1.groups()
        m1_num = _month_to_number(m1_name)
        m2_num = _month_to_number(m2_name)
        if m1_num and m2_num:
            try:
                return date(int(y1), m1_num, int(d1)), date(int(y2), m2_num, int(d2))
            except ValueError:
                return None, None

    m2 = patterns[1].match(text)
    if m2:
        d1, m1_name, d2, m2_name, y = m2.groups()
        m1_num = _month_to_number(m1_name)
        m2_num = _month_to_number(m2_name)
        if m1_num and m2_num:
            try:
                return date(int(y), m1_num, int(d1)), date(int(y), m2_num, int(d2))
            except ValueError:
                return None, None

    return None, None


def _extract_archive_event_date_ranges(archive_html: str) -> Dict[Tuple[str, str], Tuple[Optional[date], Optional[date]]]:
    ranges: Dict[Tuple[str, str], Tuple[Optional[date], Optional[date]]] = {}
    for m in re.finditer(
        r'<a href="/en/scores/archive/([^/]+)/([^/]+)/(\d{4})/results"[^>]*>[\s\S]*?<span class="Date">([\s\S]*?)</span>',
        archive_html,
        flags=re.IGNORECASE,
    ):
        slug = m.group(1).strip()
        tid = m.group(2).strip()
        date_text = re.sub(r"<[^>]+>", " ", m.group(4))
        start_date, end_date = _parse_archive_event_date_range(date_text)
        ranges[(slug, tid)] = (start_date, end_date)
    return ranges


def _infer_match_date_from_round_label(
    *,
    round_label: Optional[str],
    day_label: Optional[str],
    start_date: date,
    end_date: date,
) -> Optional[date]:
    span_days = max(0, (end_date - start_date).days)
    text = f"{round_label or ''} {day_label or ''}".strip().lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return start_date

    if "qualifying" in text or re.search(r"\bq[1-3]\b", text):
        if "3rd" in text or "q3" in text:
            return min(end_date, start_date + timedelta(days=2))
        if "2nd" in text or "q2" in text:
            return min(end_date, start_date + timedelta(days=1))
        return start_date

    if "round robin" in text or text == "rr":
        return min(end_date, start_date + timedelta(days=max(1, span_days // 2)))

    round_from_end: Optional[int] = None
    if "final" in text and "semi" not in text and "quarter" not in text and "round of" not in text:
        round_from_end = 0
    elif "semifinal" in text:
        round_from_end = 1
    elif "quarterfinal" in text:
        round_from_end = 2
    elif "round of 16" in text:
        round_from_end = 3
    elif "round of 32" in text:
        round_from_end = 4
    elif "round of 64" in text:
        round_from_end = 5
    elif "round of 128" in text:
        round_from_end = 6
    elif "4th round" in text:
        round_from_end = 3
    elif "3rd round" in text:
        round_from_end = 4
    elif "2nd round" in text:
        round_from_end = 5 if span_days >= 12 else 3
    elif "1st round" in text:
        round_from_end = 6 if span_days >= 12 else 4

    if round_from_end is None:
        return start_date

    inferred = end_date - timedelta(days=round_from_end)
    if inferred < start_date:
        return start_date
    if inferred > end_date:
        return end_date
    return inferred


def _fill_missing_match_dates_for_event(
    rows: Sequence[Dict[str, Any]],
    *,
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[Dict[str, Any]]:
    if not rows or not start_date or not end_date:
        return list(rows)

    filled: List[Dict[str, Any]] = []
    for row in rows:
        current_date = row.get("match_date")
        if current_date:
            filled.append(row)
            continue
        inferred = _infer_match_date_from_round_label(
            round_label=row.get("round_and_court"),
            day_label=row.get("day_label"),
            start_date=start_date,
            end_date=end_date,
        )
        if inferred is not None:
            row = {**row, "match_date": inferred.isoformat()}
        filled.append(row)
    return filled


def _results_archive_data_for_year(
    year: int,
) -> Tuple[List[Tuple[str, str, str]], Dict[Tuple[str, str], Tuple[Optional[date], Optional[date]]]]:
    archive_url = f"https://www.atptour.com/en/scores/results-archive?year={year}"
    archive_html = _fetch_html_url(archive_url)
    if not archive_html:
        return [], {}
    return (
        _extract_results_urls_from_archive_html(archive_html),
        _extract_archive_event_date_ranges(archive_html),
    )


def _results_archive_urls_for_year(year: int) -> List[Tuple[str, str, str]]:
    urls, _ = _results_archive_data_for_year(year)
    return urls


def _merge_results_url_sets(
    *url_sets: Sequence[Tuple[str, str, str]],
) -> List[Tuple[str, str, str]]:
    merged: Dict[str, Tuple[str, str, str]] = {}
    for url_set in url_sets:
        for slug, tid, url in url_set:
            merged[url] = (slug, tid, url)
    return list(merged.values())


def _event_window_lookup_for_urls(
    archive_ranges_by_year: Dict[int, Dict[Tuple[str, str], Tuple[Optional[date], Optional[date]]]],
    results_urls: Sequence[Tuple[str, str, str]],
) -> Dict[Tuple[str, str], Tuple[Optional[date], Optional[date]]]:
    lookup: Dict[Tuple[str, str], Tuple[Optional[date], Optional[date]]] = {}
    for slug, tid, url in results_urls:
        year = _year_from_results_url(url)
        if year is None:
            continue
        year_ranges = archive_ranges_by_year.get(year) or {}
        event_window = year_ranges.get((slug, tid))
        if event_window:
            lookup[(slug, tid)] = event_window
    return lookup


def _round_sort_rank(round_and_court: Optional[str]) -> int:
    text = (round_and_court or "").lower()
    if not text:
        return 999
    mapping = [
        ("final", 1),
        ("semi", 2),
        ("quarter", 3),
        ("round of 16", 4),
        ("round of 32", 5),
        ("round of 64", 6),
        ("round of 128", 7),
        ("2nd round qualifying", 8),
        ("1st round qualifying", 9),
        ("qualifying", 10),
    ]
    for token, rank in mapping:
        if token in text:
            return rank
    return 50


def _populate_missing_match_dates_from_event_windows(
    rows: Sequence[Dict[str, Any]],
    *,
    event_windows: Dict[Tuple[str, str], Tuple[Optional[date], Optional[date]]],
) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    output: List[Dict[str, Any]] = []
    for row in rows:
        cloned = dict(row)
        output.append(cloned)
        if cloned.get("match_date"):
            continue
        key = (str(cloned.get("tournament_slug") or ""), str(cloned.get("tournament_id") or ""))
        grouped.setdefault(key, []).append(cloned)

    for key, group_rows in grouped.items():
        start_date, end_date = event_windows.get(key, (None, None))
        if not start_date or not end_date or end_date < start_date:
            continue

        total_days = (end_date - start_date).days + 1
        if total_days <= 0:
            continue

        by_round: Dict[str, List[Dict[str, Any]]] = {}
        for row in group_rows:
            round_key = (row.get("round_and_court") or "").strip() or "Unknown"
            by_round.setdefault(round_key, []).append(row)

        round_order = sorted(
            by_round.keys(),
            key=lambda round_name: (_round_sort_rank(round_name), round_name),
        )

        denominator = max(1, len(round_order) - 1)
        for index, round_name in enumerate(round_order):
            offset = round((index / denominator) * (total_days - 1))
            assigned = start_date + timedelta(days=offset)
            assigned_iso = assigned.isoformat()
            for row in by_round[round_name]:
                row["match_date"] = assigned_iso

    return output


def _dedupe_match_results_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """De-duplicate normalized match result rows before BigQuery insert."""
    deduped: List[Dict[str, Any]] = []
    seen: Set[Tuple[Any, ...]] = set()
    for row in rows:
        key = (
            row.get("tournament_id"),
            row.get("tournament_slug"),
            str(row.get("match_date")),
            row.get("round_and_court"),
            row.get("player_1_name"),
            row.get("player_2_name"),
            row.get("player_1_scores"),
            row.get("player_2_scores"),
            row.get("match_duration"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


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
        # Also clear match results and H2H so the live re-fetches don't create
        # duplicate rows on each daily run.
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
    # Try to refresh the daily schedule from ATP Tour so we always show today's matches.
    # The slug/id come from the capture file URL, which identifies the active tournament.
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
    # All schedule rows (including past matches) — used to build H2H pairs and player IDs.
    # The date filter is intentionally skipped here so that a schedule file captured on a
    # previous day still produces valid player pairs for H2H and stats lookups.
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
        # Only truly upcoming matches go into website_upcoming_matches.
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
    # Use all_schedule_rows (includes past matches) so a stale schedule file still produces pairs.
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

    results_capture = captures["match_results"]
    result_slug, result_tid = _extract_slug_and_tournament_id(results_capture.get("request_url"))
    # Try to fetch the current tournament's results page live.  The results page on ATP Tour
    # always shows ALL completed matches for the current tournament (not just one day), so
    # fetching it fresh gives us the complete running history for the active event.
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
        # website_match_results is the historical per-match results table (one row per match).
        # The raw HTML payload is already stored in website_raw_responses (endpoint_key="match_results").
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

    # Re-fetch results across tournament calendar URLs so website_match_results_rows
    # stays season-complete rather than containing only the active event.
    if isinstance(tournament_dates["payload_json"], dict):
        calendar_results_urls = _results_urls_for_year_range(
            tournament_dates["payload_json"],
            start_year=start_year,
            end_year=end_year,
        )
        archive_results_urls: List[Tuple[str, str, str]] = []
        archive_event_ranges: Dict[Tuple[str, str], Tuple[Optional[date], Optional[date]]] = {}
        for year in range(start_year, end_year + 1):
            year_urls, year_ranges = _results_archive_data_for_year(year)
            archive_results_urls.extend(year_urls)
            archive_event_ranges.update(year_ranges)
        results_urls = _merge_results_url_sets(calendar_results_urls, archive_results_urls)

        for past_slug, past_tid, past_url in results_urls:
            if past_slug == result_slug and past_tid == result_tid:
                continue  # current tournament already parsed above
            past_html = _fetch_html_url(past_url)
            if not past_html:
                continue
            event_rows = [
                row.to_dict()
                for row in normalize_match_results_html(
                    past_slug,
                    past_tid,
                    past_html,
                    snapshot_ts_utc=snapshot_ts,
                )
            ]
            event_start, event_end = archive_event_ranges.get((past_slug, past_tid), (None, None))
            event_rows = _fill_missing_match_dates_for_event(
                event_rows,
                start_date=event_start,
                end_date=event_end,
            )
            parsed_match_results_rows.extend(event_rows)

    parsed_match_results_rows = _dedupe_match_results_rows(parsed_match_results_rows)

    player_ids: Set[str] = set()
    # Use all_schedule_rows (includes past matches) so a stale schedule file still yields player IDs.
    for row in all_schedule_rows:
        p1 = _extract_player_id_from_profile_url(row.get("player_1_profile_url"))
        p2 = _extract_player_id_from_profile_url(row.get("player_2_profile_url"))
        if p1:
            player_ids.add(p1)
        if p2:
            player_ids.add(p2)
    # Also mine draws HTML — the full bracket contains every player in the tournament draw.
    draws_html = draws_capture.get("payload_text") or ""
    if draws_html:
        player_ids |= _extract_player_ids_from_html(draws_html)
    # Also extract from the results HTML — every player in a completed match has a profile link.
    results_html_text = results_capture.get("payload_text") or ""
    if results_html_text:
        player_ids |= _extract_player_ids_from_html(results_html_text)
    # Also include seeded players from who_is_playing.
    who_payload_for_ids = captures["who_is_playing"].get("payload_json")
    if isinstance(who_payload_for_ids, dict):
        for player in who_payload_for_ids.get("PlayersList", []) or []:
            pid = _extract_player_id_from_profile_url(player.get("ProfileUrl"))
            if pid:
                player_ids.add(pid)

    default_stats_captures = {
        "all": captures["player_stats_all"],
        "clay": captures["player_stats_clay"],
        "grass": captures["player_stats_grass"],
        "hard": captures["player_stats_hard"],
    }
    surface_path = {
        "all": "all",
        "clay": "Clay",
        "grass": "Grass",
        "hard": "Hard",
    }

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
        # website_head_to_head now stores individual historical H2H match rows (one per match),
        # mirroring the pattern used for website_match_results.
        # The raw JSON blobs (h2h_rows) are already in website_raw_responses.
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
