from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Iterable, List, Optional

from .client import AtpApiError, fetch_paginated


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _default_season() -> int:
    return datetime.now(timezone.utc).year


def _collect_matches(
    *,
    season: int,
    tournament_ids: Optional[List[int]],
    round_name: Optional[str],
    per_page: int,
    max_pages: Optional[int],
) -> List[dict[str, Any]]:
    params: dict[str, Any] = {"season": season}
    if tournament_ids:
        params["tournament_ids[]"] = tournament_ids
    if round_name:
        params["round"] = round_name

    return fetch_paginated(
        "/matches",
        params=params,
        per_page=per_page,
        max_pages=max_pages,
    )


def _is_upcoming(match: dict[str, Any], cutoff: datetime) -> bool:
    scheduled = _parse_iso_datetime(match.get("scheduled_time"))
    if scheduled is None:
        return False
    return scheduled >= cutoff


def _format_row(match: dict[str, Any]) -> dict[str, Any]:
    tournament = match.get("tournament") or {}
    player1 = match.get("player1") or {}
    player2 = match.get("player2") or {}
    scheduled = _parse_iso_datetime(match.get("scheduled_time"))
    return {
        "id": match.get("id"),
        "tournament": tournament.get("name"),
        "round": match.get("round"),
        "player_1": player1.get("full_name"),
        "player_2": player2.get("full_name"),
        "scheduled_time_utc": scheduled.isoformat() if scheduled else None,
        "not_before_text": match.get("not_before_text"),
    }


def _render_table(rows: Iterable[dict[str, Any]]) -> str:
    columns = [
        "id",
        "tournament",
        "round",
        "player_1",
        "player_2",
        "scheduled_time_utc",
        "not_before_text",
    ]
    rows_list = list(rows)
    widths = {col: len(col) for col in columns}

    for row in rows_list:
        for col in columns:
            widths[col] = max(widths[col], len(str(row.get(col, ""))))

    header = " | ".join(col.ljust(widths[col]) for col in columns)
    divider = "-+-".join("-" * widths[col] for col in columns)
    lines = [header, divider]

    for row in rows_list:
        line = " | ".join(str(row.get(col, "")).ljust(widths[col]) for col in columns)
        lines.append(line)

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch upcoming ATP matches from the BallDontLie ATP API.",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=_default_season(),
        help="Season year (default: current year).",
    )
    parser.add_argument(
        "--tournament-id",
        action="append",
        type=int,
        dest="tournament_ids",
        help="Tournament ID (repeatable).",
    )
    parser.add_argument(
        "--round",
        dest="round_name",
        help="Round name (e.g., Finals, Semi-Finals).",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Results per page (max 100).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional maximum number of pages to fetch.",
    )
    parser.add_argument(
        "--from",
        dest="from_time",
        default=None,
        help="Filter matches scheduled at or after this ISO timestamp (UTC).",
    )
    parser.add_argument(
        "--output",
        choices=("table", "json"),
        default="table",
        help="Output format.",
    )

    args = parser.parse_args()
    cutoff = _parse_iso_datetime(args.from_time) or datetime.now(timezone.utc)

    try:
        matches = _collect_matches(
            season=args.season,
            tournament_ids=args.tournament_ids,
            round_name=args.round_name,
            per_page=args.per_page,
            max_pages=args.max_pages,
        )
    except AtpApiError as exc:
        print(f"Error: {exc}")
        return 1

    upcoming = [m for m in matches if _is_upcoming(m, cutoff)]
    formatted = sorted(
        (_format_row(match) for match in upcoming),
        key=lambda row: row.get("scheduled_time_utc") or "",
    )

    if args.output == "json":
        print(json.dumps(formatted, indent=2))
    else:
        print(_render_table(formatted))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
