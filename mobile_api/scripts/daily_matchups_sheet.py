from __future__ import annotations

"""Export daily PGA and ATP matchups to Google Sheets.

Example:
  python -m mobile_api.scripts.daily_matchups_sheet \
    --sheet-id "<SHEET_ID>" \
    --service-account "$GCP_SERVICE_ACCOUNT"
"""

import argparse
import json
import os
import re
import sys
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import gspread
from google.oauth2 import service_account

from mobile_api.atp.client import AtpApiError, fetch_paginated as fetch_atp_paginated
from mobile_api.pga.client import PgaApiError, fetch_paginated as fetch_pga_paginated
from mobile_api.pga.utils import parse_iso_datetime

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

PGA_HEADERS = [
    "date",
    "tournament",
    "round",
    "tee_time",
    "group",
    "player_1",
    "player_2",
    "player_3",
    "player_4",
    "players",
]

ATP_HEADERS = [
    "date",
    "tournament",
    "round",
    "surface",
    "start_time",
    "player_1",
    "player_2",
    "match_status",
    "is_live",
    "score",
    "match_id",
]

SHEET_ID_RE = re.compile(r"/d/([a-zA-Z0-9-_]+)")
ACTIVE_TOURNAMENT_STATUSES = {"in_progress", "active", "ongoing", "live"}


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(raw).date()
        except ValueError:
            try:
                return date.fromisoformat(raw[:10])
            except ValueError:
                return None
    return None


def _parse_sheet_id(sheet_id: Optional[str], sheet_url: Optional[str]) -> str:
    if sheet_id:
        return sheet_id
    if sheet_url:
        match = SHEET_ID_RE.search(sheet_url)
        if match:
            return match.group(1)
        raise ValueError("Unable to parse sheet id from URL.")
    env_value = os.getenv("MATCHUPS_SHEET_ID") or os.getenv("SPREADSHEET_ID")
    if env_value:
        return env_value
    raise ValueError("Missing sheet id. Set --sheet-id or MATCHUPS_SHEET_ID.")


def _load_credentials(service_account_raw: Optional[str]) -> service_account.Credentials:
    raw = service_account_raw
    if raw is None:
        for key in (
            "GCP_SERVICE_ACCOUNT",
            "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON",
            "GOOGLE_APPLICATION_CREDENTIALS_JSON",
        ):
            env_value = os.getenv(key)
            if env_value:
                raw = env_value
                break

    if raw:
        try:
            payload = json.loads(raw)
            return service_account.Credentials.from_service_account_info(
                payload,
                scopes=SHEETS_SCOPES,
            )
        except json.JSONDecodeError:
            if os.path.exists(raw):
                return service_account.Credentials.from_service_account_file(
                    raw,
                    scopes=SHEETS_SCOPES,
                )
            raise ValueError("Service account payload is not JSON or a file path.")

    file_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if file_path and os.path.exists(file_path):
        return service_account.Credentials.from_service_account_file(
            file_path,
            scopes=SHEETS_SCOPES,
        )

    raise ValueError(
        "Missing service account credentials. Provide --service-account or "
        "set GOOGLE_APPLICATION_CREDENTIALS or GCP_SERVICE_ACCOUNT."
    )


def _gspread_client(service_account_raw: Optional[str]) -> gspread.Client:
    creds = _load_credentials(service_account_raw)
    return gspread.authorize(creds)


def _resolve_target_date(value: Optional[str], tz_name: str) -> date:
    if value:
        return date.fromisoformat(value)
    tz = ZoneInfo(tz_name)
    return datetime.now(tz).date()


def _worksheet(spreadsheet: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=1000, cols=20)


def _write_rows(
    worksheet: gspread.Worksheet,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    values = [list(headers)]
    for row in rows:
        values.append([row.get(header, "") for header in headers])
    worksheet.clear()
    worksheet.update(values, value_input_option="RAW")
    worksheet.freeze(rows=1)
    worksheet.resize(rows=len(values), cols=len(headers))


def _parse_atp_match_date(match: Dict[str, Any], target_date: date) -> Optional[date]:
    for key in (
        "start_date",
        "start_time",
        "date",
        "match_date",
        "scheduled_time",
        "start_at",
        "utc_start",
        "played_at",
    ):
        parsed = _parse_date(match.get(key))
        if parsed:
            return parsed

    tournament = match.get("tournament") or {}
    start = _parse_date(tournament.get("start_date"))
    end = _parse_date(tournament.get("end_date"))
    if start and end and start <= target_date <= end:
        return target_date
    if start == target_date or end == target_date:
        return target_date
    return None


def _player_name(player: Dict[str, Any]) -> str:
    return (
        player.get("full_name")
        or player.get("display_name")
        or " ".join(filter(None, [player.get("first_name"), player.get("last_name")])).strip()
        or ""
    )


def fetch_atp_matchups(
    target_date: date,
    *,
    max_pages: int,
) -> Tuple[List[Dict[str, Any]], str]:
    attempts = [
        {"date": target_date.isoformat()},
        {"dates[]": [target_date.isoformat()]},
        {"start_date": target_date.isoformat()},
        {"start_date_gte": target_date.isoformat(), "start_date_lte": target_date.isoformat()},
        {"match_date": target_date.isoformat()},
    ]

    for params in attempts:
        try:
            matches = fetch_atp_paginated(
                "/matches",
                params=params,
                cache_ttl=300,
                max_pages=max_pages,
            )
        except AtpApiError as exc:
            message = str(exc)
            if "Route not found" in message or "400" in message:
                continue
            raise
        if matches:
            return _normalize_atp_matches(matches, target_date), f"params={params}"

    fallback_matches = fetch_atp_paginated(
        "/matches",
        params={"season": target_date.year},
        cache_ttl=300,
        max_pages=max_pages,
    )
    return _normalize_atp_matches(fallback_matches, target_date), "season_fallback"


def _normalize_atp_matches(matches: Iterable[Dict[str, Any]], target_date: date) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for match in matches:
        tournament = match.get("tournament") or {}
        match_date = _parse_atp_match_date(match, target_date)
        if match_date and match_date != target_date:
            continue
        if match_date is None:
            continue
        player1 = match.get("player1") or {}
        player2 = match.get("player2") or {}
        rows.append(
            {
                "date": match_date.isoformat(),
                "tournament": tournament.get("name"),
                "round": match.get("round"),
                "surface": tournament.get("surface"),
                "start_time": match.get("start_time") or match.get("start_date"),
                "player_1": _player_name(player1),
                "player_2": _player_name(player2),
                "match_status": match.get("match_status"),
                "is_live": match.get("is_live"),
                "score": match.get("score"),
                "match_id": match.get("id"),
            }
        )
    return sorted(rows, key=lambda row: (row.get("tournament") or "", row.get("round") or ""))


def _tournament_date_bounds(tournament: Dict[str, Any]) -> Tuple[Optional[date], Optional[date]]:
    start = parse_iso_datetime(tournament.get("start_date"))
    end = parse_iso_datetime(tournament.get("end_date"))
    return (start.date() if start else None, end.date() if end else None)


def _active_pga_tournaments(
    tournaments: Iterable[Dict[str, Any]],
    *,
    target_date: date,
) -> List[Dict[str, Any]]:
    active: List[Dict[str, Any]] = []
    for tournament in tournaments:
        start, end = _tournament_date_bounds(tournament)
        status = (tournament.get("status") or "").strip().lower()
        if start and end and start <= target_date <= end:
            active.append(tournament)
            continue
        if status in ACTIVE_TOURNAMENT_STATUSES:
            active.append(tournament)
    return active


def _infer_round_number(start_date: Optional[date], target_date: date) -> Optional[int]:
    if not start_date:
        return None
    delta = (target_date - start_date).days
    if delta < 0:
        return None
    round_number = delta + 1
    return round_number if 1 <= round_number <= 6 else None


def _extract_group_key(record: Dict[str, Any], fallback: str) -> str:
    for key in (
        "group_id",
        "pairing_id",
        "group_number",
        "group",
        "pairing",
        "tee_time",
        "start_time",
    ):
        value = record.get(key)
        if value is not None:
            return f"{key}:{value}"
    return fallback


def _extract_tee_time(record: Dict[str, Any]) -> Optional[str]:
    tee_time = record.get("tee_time") or record.get("start_time")
    if isinstance(tee_time, dict):
        return tee_time.get("time") or tee_time.get("local") or tee_time.get("utc")
    if isinstance(tee_time, str):
        return tee_time
    return None


def _extract_round_label(record: Dict[str, Any]) -> Optional[str]:
    round_value = record.get("round_number") or record.get("round") or record.get("round_num")
    if round_value is None:
        return None
    return str(round_value)


def _extract_pga_player(record: Dict[str, Any]) -> str:
    player = record.get("player") or {}
    name = _player_name(player)
    if name:
        return name
    return (
        record.get("player_display_name")
        or " ".join(
            filter(
                None,
                [
                    record.get("player_first_name"),
                    record.get("player_last_name"),
                ],
            )
        ).strip()
        or ""
    )


def _normalize_pga_rounds(
    tournament: Dict[str, Any],
    records: Iterable[Dict[str, Any]],
    *,
    target_date: date,
) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for index, record in enumerate(records):
        fallback = f"row-{index + 1}"
        group_key = _extract_group_key(record, fallback)
        if group_key not in groups:
            groups[group_key] = {
                "players": [],
                "round": _extract_round_label(record),
                "tee_time": _extract_tee_time(record),
                "group": group_key.split(":", 1)[-1] if ":" in group_key else "",
            }
            order.append(group_key)
        player_name = _extract_pga_player(record)
        if player_name and player_name not in groups[group_key]["players"]:
            groups[group_key]["players"].append(player_name)
        if not groups[group_key]["round"]:
            groups[group_key]["round"] = _extract_round_label(record)
        if not groups[group_key]["tee_time"]:
            groups[group_key]["tee_time"] = _extract_tee_time(record)

    rows: List[Dict[str, Any]] = []
    for key in order:
        group = groups[key]
        players = group["players"]
        row = {
            "date": target_date.isoformat(),
            "tournament": tournament.get("name"),
            "round": group.get("round"),
            "tee_time": group.get("tee_time"),
            "group": group.get("group"),
            "players": " / ".join(players),
        }
        for idx in range(4):
            row[f"player_{idx + 1}"] = players[idx] if idx < len(players) else ""
        rows.append(row)
    return rows


def fetch_pga_matchups(
    target_date: date,
    *,
    season: Optional[int],
    source: str,
    max_pages: int,
) -> Tuple[List[Dict[str, Any]], str]:
    season = season or target_date.year
    tournaments = fetch_pga_paginated(
        "/tournaments",
        params={"season": season},
        cache_ttl=300,
        max_pages=max_pages,
        source=source,
    )

    active = _active_pga_tournaments(tournaments, target_date=target_date)
    if not active:
        return [], "no_active_tournaments"

    rows: List[Dict[str, Any]] = []
    mode = "tournament_rounds"
    for tournament in active:
        tournament_id = tournament.get("id")
        if not tournament_id:
            continue
        start_date, _ = _tournament_date_bounds(tournament)
        inferred_round = _infer_round_number(start_date, target_date)
        params_list = [
            {"tournament_ids": [tournament_id], "date": target_date.isoformat()},
            {"tournament_ids": [tournament_id], "round_date": target_date.isoformat()},
            {"tournament_ids": [tournament_id], "round_number": inferred_round}
            if inferred_round
            else None,
            {"tournament_ids": [tournament_id]},
        ]

        records: List[Dict[str, Any]] = []
        for params in filter(None, params_list):
            try:
                records = fetch_pga_paginated(
                    "/tournament_rounds",
                    params=params,
                    cache_ttl=300,
                    max_pages=max_pages,
                    source=source,
                )
            except PgaApiError as exc:
                message = str(exc)
                if "Route not found" in message or "404" in message:
                    records = []
                    mode = "tournament_results_fallback"
                    break
                raise
            if records:
                break

        if not records:
            records = fetch_pga_paginated(
                "/tournament_results",
                params={"tournament_ids": [tournament_id]},
                cache_ttl=300,
                max_pages=max_pages,
                source=source,
            )
            mode = "tournament_results_fallback"

        rows.extend(_normalize_pga_rounds(tournament, records, target_date=target_date))

    return rows, mode


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Dump today's PGA and ATP matchups to Google Sheets.",
    )
    parser.add_argument("--date", help="Target date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--tz", default="America/New_York", help="Timezone for today.")
    parser.add_argument("--sheet-id", help="Google Sheet ID.")
    parser.add_argument("--sheet-url", help="Google Sheet URL.")
    parser.add_argument("--pga-tab", default="Golf Matchups", help="Worksheet name for PGA.")
    parser.add_argument("--atp-tab", default="Tennis Matchups", help="Worksheet name for ATP.")
    parser.add_argument("--service-account", help="Service account JSON (string or file path).")
    parser.add_argument("--pga-source", default="api", help="PGA source: api or bq.")
    parser.add_argument("--pga-season", type=int, help="Season override for PGA.")
    parser.add_argument("--max-pages", type=int, default=10, help="Max pages per API call.")
    parser.add_argument("--dry-run", action="store_true", help="Print rows, skip sheets.")

    args = parser.parse_args(argv)

    target_date = _resolve_target_date(args.date, args.tz)
    sheet_id = _parse_sheet_id(args.sheet_id, args.sheet_url)

    print(f"[matchups] target_date={target_date.isoformat()}")

    try:
        pga_rows, pga_mode = fetch_pga_matchups(
            target_date,
            season=args.pga_season,
            source=args.pga_source,
            max_pages=args.max_pages,
        )
        print(f"[matchups] PGA rows={len(pga_rows)} mode={pga_mode}")
    except Exception as exc:
        print(f"[matchups] PGA failed: {exc}")
        pga_rows = []

    try:
        atp_rows, atp_mode = fetch_atp_matchups(target_date, max_pages=args.max_pages)
        print(f"[matchups] ATP rows={len(atp_rows)} mode={atp_mode}")
    except Exception as exc:
        print(f"[matchups] ATP failed: {exc}")
        atp_rows = []

    if args.dry_run:
        print("[matchups] Dry run enabled.")
        print(f"PGA rows: {len(pga_rows)}")
        print(f"ATP rows: {len(atp_rows)}")
        return 0

    gc = _gspread_client(args.service_account)
    spreadsheet = gc.open_by_key(sheet_id)
    pga_sheet = _worksheet(spreadsheet, args.pga_tab)
    atp_sheet = _worksheet(spreadsheet, args.atp_tab)

    _write_rows(pga_sheet, PGA_HEADERS, pga_rows)
    _write_rows(atp_sheet, ATP_HEADERS, atp_rows)

    print("[matchups] Sheets updated successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
