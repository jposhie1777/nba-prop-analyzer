from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

DAILY_SCHEDULE_URL = "https://eng.koreabaseball.com/Schedule/DailySchedule.aspx"

BTN_BEFORE = "ctl00$ctl00$ctl00$ctl00$cphContainer$cphContainer$cphContent$cphContent$btnBefore"


@dataclass(frozen=True)
class MonthCursor:
    year: int
    month: int

    @classmethod
    def parse(cls, value: str) -> "MonthCursor":
        y, m = value.split("-")
        return cls(int(y), int(m))

    def key(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def _month_compare(a: MonthCursor, b: MonthCursor) -> int:
    if (a.year, a.month) < (b.year, b.month):
        return -1
    if (a.year, a.month) > (b.year, b.month):
        return 1
    return 0


def _month_prev(cur: MonthCursor) -> MonthCursor:
    if cur.month == 1:
        return MonthCursor(cur.year - 1, 12)
    return MonthCursor(cur.year, cur.month - 1)


def _strip_html(text: str) -> str:
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _extract_hidden_inputs(html: str) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        match = re.search(rf'name="{name}"[^>]*value="([^"]*)"', html)
        fields[name] = match.group(1) if match else ""
    return fields


def _extract_month(html: str) -> MonthCursor:
    match = re.search(r'id="cphContainer_cphContainer_cphContent_cphContent_lblGameMonth">\s*(\d{4})\.(\d{2})\s*<', html)
    if not match:
        raise RuntimeError("Unable to parse current month from KBO DailySchedule page")
    return MonthCursor(int(match.group(1)), int(match.group(2)))


def _extract_tbody(html: str) -> str:
    match = re.search(r"<tbody>(.*?)</tbody>", html, flags=re.S | re.I)
    return match.group(1) if match else ""


def _parse_score(score_text: str) -> Tuple[Optional[int], Optional[int], str, str]:
    score_text = score_text.strip()
    if not score_text or score_text == ":":
        return None, None, "pending", "scheduled"

    match = re.fullmatch(r"(\d+)\s*:\s*(\d+)", score_text)
    if not match:
        return None, None, "unknown", "unknown"

    away_runs = int(match.group(1))
    home_runs = int(match.group(2))
    if away_runs > home_runs:
        return away_runs, home_runs, "away_win", "final"
    if home_runs > away_runs:
        return away_runs, home_runs, "home_win", "final"
    return away_runs, home_runs, "tie", "final"


def parse_games_from_html(html: str) -> List[Dict[str, Any]]:
    month = _extract_month(html)
    body = _extract_tbody(html)
    rows = re.findall(r"<tr>(.*?)</tr>", body, flags=re.S | re.I)

    current_date: Optional[str] = None
    current_type: Optional[str] = None
    games: List[Dict[str, Any]] = []

    for row_html in rows:
        cells = re.findall(r"<td([^>]*)>(.*?)</td>", row_html, flags=re.S | re.I)
        if not cells:
            continue

        time_txt = ""
        location = ""
        etc = ""
        game_cells: List[str] = []

        for attrs, raw in cells:
            attrs_lower = attrs.lower()
            text = _strip_html(raw)

            if 'title="date"' in attrs_lower:
                current_date = text
                continue
            if 'title="type"' in attrs_lower:
                current_type = text
                continue
            if 'title="game"' in attrs_lower:
                game_cells.append(text)
                continue
            if 'class="time"' in attrs_lower:
                time_txt = text
                continue
            if 'class="location"' in attrs_lower:
                location = text
                continue
            if 'class="etc"' in attrs_lower:
                etc = text
                continue

        if not current_date or len(game_cells) < 3:
            continue

        day_match = re.match(r"(\d{2})\.(\d{2})", current_date)
        if not day_match:
            continue

        game_date = date(month.year, int(day_match.group(1)), int(day_match.group(2)))
        away_team, score_text, home_team = game_cells[0], game_cells[1], game_cells[2]

        away_runs, home_runs, outcome, status = _parse_score(score_text)
        if etc.upper() == "POSTPONED":
            outcome = "postponed"
            status = "postponed"
            away_runs = None
            home_runs = None

        games.append(
            {
                "season": game_date.year,
                "game_date": game_date.isoformat(),
                "game_type": current_type or "",
                "game_time": time_txt,
                "away_team": away_team,
                "home_team": home_team,
                "away_runs": away_runs,
                "home_runs": home_runs,
                "outcome": outcome,
                "status": status,
                "location": location,
                "notes": etc,
            }
        )

    return games


def fetch_monthly_games(start_month: MonthCursor, end_month: MonthCursor) -> List[Dict[str, Any]]:
    session = requests.Session()
    response = session.get(DAILY_SCHEDULE_URL, timeout=30)
    response.raise_for_status()
    html = response.text

    all_games: List[Dict[str, Any]] = []

    while True:
        current_month = _extract_month(html)

        if _month_compare(current_month, end_month) <= 0 and _month_compare(current_month, start_month) >= 0:
            all_games.extend(parse_games_from_html(html))

        if _month_compare(current_month, start_month) <= 0:
            break

        hidden = _extract_hidden_inputs(html)
        payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
            f"{BTN_BEFORE}.x": "1",
            f"{BTN_BEFORE}.y": "1",
        }

        response = session.post(DAILY_SCHEDULE_URL, data=payload, timeout=30)
        response.raise_for_status()
        html = response.text

    deduped: Dict[str, Dict[str, Any]] = {}
    for game in all_games:
        key = "|".join(
            [
                game["game_date"],
                game.get("game_time", ""),
                game.get("away_team", ""),
                game.get("home_team", ""),
                game.get("location", ""),
                game.get("game_type", ""),
            ]
        )
        game["game_key"] = key
        deduped[key] = game

    return list(deduped.values())


def build_team_summary(games: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_team: Dict[Tuple[int, str], Dict[str, Any]] = {}

    for game in games:
        if game.get("status") != "final":
            continue

        season = int(game["season"])
        away_team = game["away_team"]
        home_team = game["home_team"]
        away_runs = int(game["away_runs"])
        home_runs = int(game["home_runs"])

        for team in (away_team, home_team):
            key = (season, team)
            if key not in by_team:
                by_team[key] = {
                    "season": season,
                    "team": team,
                    "games_played": 0,
                    "wins": 0,
                    "losses": 0,
                    "ties": 0,
                    "runs_scored": 0,
                    "runs_allowed": 0,
                }

        away_row = by_team[(season, away_team)]
        home_row = by_team[(season, home_team)]

        away_row["games_played"] += 1
        home_row["games_played"] += 1
        away_row["runs_scored"] += away_runs
        away_row["runs_allowed"] += home_runs
        home_row["runs_scored"] += home_runs
        home_row["runs_allowed"] += away_runs

        if away_runs > home_runs:
            away_row["wins"] += 1
            home_row["losses"] += 1
        elif home_runs > away_runs:
            home_row["wins"] += 1
            away_row["losses"] += 1
        else:
            away_row["ties"] += 1
            home_row["ties"] += 1

    summary: List[Dict[str, Any]] = []
    for (_, _), row in sorted(by_team.items(), key=lambda item: (item[0][0], item[0][1])):
        gp = row["games_played"] or 1
        row["avg_runs_scored"] = round(row["runs_scored"] / gp, 3)
        row["avg_runs_allowed"] = round(row["runs_allowed"] / gp, 3)
        summary.append(row)

    return summary


def _get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _dataset() -> str:
    return os.getenv("KBO_DATASET", "kbo_data")


def _location() -> str:
    return os.getenv("KBO_BQ_LOCATION", "US")


def _table_id(client: bigquery.Client, table_name: str) -> str:
    return f"{client.project}.{_dataset()}.{table_name}"


def ensure_dataset_and_tables(client: bigquery.Client) -> None:
    dataset_id = f"{client.project}.{_dataset()}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        ds = bigquery.Dataset(dataset_id)
        ds.location = _location()
        client.create_dataset(ds)

    try:
        client.get_table(_table_id(client, "games"))
    except NotFound:
        schema = [
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ingest_run_id", "STRING"),
            bigquery.SchemaField("season", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("game_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("game_type", "STRING"),
            bigquery.SchemaField("game_time", "STRING"),
            bigquery.SchemaField("away_team", "STRING"),
            bigquery.SchemaField("home_team", "STRING"),
            bigquery.SchemaField("away_runs", "INT64"),
            bigquery.SchemaField("home_runs", "INT64"),
            bigquery.SchemaField("outcome", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("notes", "STRING"),
            bigquery.SchemaField("game_key", "STRING"),
        ]
        table = bigquery.Table(_table_id(client, "games"), schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="game_date",
        )
        table.clustering_fields = ["season", "away_team", "home_team"]
        client.create_table(table)

    try:
        client.get_table(_table_id(client, "team_summary"))
    except NotFound:
        schema = [
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ingest_run_id", "STRING"),
            bigquery.SchemaField("season", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("team", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("games_played", "INT64"),
            bigquery.SchemaField("wins", "INT64"),
            bigquery.SchemaField("losses", "INT64"),
            bigquery.SchemaField("ties", "INT64"),
            bigquery.SchemaField("runs_scored", "INT64"),
            bigquery.SchemaField("runs_allowed", "INT64"),
            bigquery.SchemaField("avg_runs_scored", "FLOAT64"),
            bigquery.SchemaField("avg_runs_allowed", "FLOAT64"),
        ]
        table = bigquery.Table(_table_id(client, "team_summary"), schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingested_at",
        )
        table.clustering_fields = ["season", "team"]
        client.create_table(table)


def _truncate_in_period(client: bigquery.Client, table: str, start_month: MonthCursor, end_month: MonthCursor) -> None:
    start_date = date(start_month.year, start_month.month, 1)
    end_date = date(end_month.year, end_month.month, 28)
    # Advance to next month and subtract one day to get true end-of-month.
    if end_date.month == 12:
        next_month = date(end_date.year + 1, 1, 1)
    else:
        next_month = date(end_date.year, end_date.month + 1, 1)
    end_date = next_month - timedelta(days=1)

    if table == "games":
        query = (
            f"DELETE FROM `{_table_id(client, table)}` "
            "WHERE game_date BETWEEN @start_date AND @end_date"
        )
    else:
        query = (
            f"DELETE FROM `{_table_id(client, table)}` "
            "WHERE season BETWEEN @start_year AND @end_year"
        )

    params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        bigquery.ScalarQueryParameter("start_year", "INT64", start_month.year),
        bigquery.ScalarQueryParameter("end_year", "INT64", end_month.year),
    ]
    client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def _insert_rows(client: bigquery.Client, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    table_id = _table_id(client, table)
    chunk_size = 500
    for idx in range(0, len(rows), chunk_size):
        errors = client.insert_rows_json(table_id, rows[idx : idx + chunk_size])
        if errors:
            raise RuntimeError(f"Failed writing rows to {table_id}: {errors[:3]}")
    return len(rows)


def run_backfill(
    start_month: MonthCursor,
    end_month: MonthCursor,
    dry_run: bool = False,
    truncate_first: bool = False,
) -> Dict[str, Any]:
    games = fetch_monthly_games(start_month=start_month, end_month=end_month)
    summary = build_team_summary(games)

    result: Dict[str, Any] = {
        "start_month": start_month.key(),
        "end_month": end_month.key(),
        "games_fetched": len(games),
        "team_summary_rows": len(summary),
        "dry_run": dry_run,
        "truncate_first": truncate_first,
    }

    if dry_run:
        return result

    client = _get_bq_client()
    ensure_dataset_and_tables(client)

    if truncate_first:
        _truncate_in_period(client, "games", start_month, end_month)
        _truncate_in_period(client, "team_summary", start_month, end_month)

    now = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())

    game_rows = [
        {
            "ingested_at": now,
            "ingest_run_id": run_id,
            **row,
        }
        for row in games
    ]
    summary_rows = [
        {
            "ingested_at": now,
            "ingest_run_id": run_id,
            **row,
        }
        for row in summary
    ]

    result["games_written"] = _insert_rows(client, "games", game_rows)
    result["team_summary_written"] = _insert_rows(client, "team_summary", summary_rows)
    result["ingest_run_id"] = run_id

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="KBO DailySchedule backfill")
    parser.add_argument("--start-month", default=None, help="First month to include (YYYY-MM)")
    parser.add_argument("--end-month", default=None, help="Last month to include (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and parse only; do not write to BigQuery")
    parser.add_argument(
        "--truncate-first",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Delete existing rows in selected period before writing",
    )
    args = parser.parse_args()

    today = datetime.now(timezone.utc)
    end = MonthCursor(today.year, today.month)
    start = MonthCursor(today.year - 2, today.month)

    if args.start_month:
        start = MonthCursor.parse(args.start_month)
    if args.end_month:
        end = MonthCursor.parse(args.end_month)

    if _month_compare(start, end) > 0:
        raise SystemExit("start-month must be <= end-month")

    result = run_backfill(start, end, dry_run=args.dry_run, truncate_first=args.truncate_first)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
