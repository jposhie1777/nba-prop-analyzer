from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo


DEFAULT_CUTOFF_TIMEZONE = ZoneInfo("America/New_York")


def parse_match_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_match_time_iso(value: Optional[str]) -> Optional[str]:
    parsed = parse_match_time(value)
    return parsed.isoformat() if parsed else None


def default_new_york_day_cutoff(now: Optional[datetime] = None) -> datetime:
    current_utc = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
    current_ny = current_utc.astimezone(DEFAULT_CUTOFF_TIMEZONE)
    midnight_ny = datetime(
        current_ny.year,
        current_ny.month,
        current_ny.day,
        tzinfo=DEFAULT_CUTOFF_TIMEZONE,
    )
    # Keep cutoff in UTC to match normalized scheduled_time values.
    return midnight_ny.astimezone(timezone.utc)


def resolve_cutoff_time(cutoff_time: Optional[str], *, now: Optional[datetime] = None) -> datetime:
    if cutoff_time is None:
        return default_new_york_day_cutoff(now)
    parsed = parse_match_time(cutoff_time)
    if parsed is None:
        raise ValueError(
            "Invalid cutoff_time value. Expected ISO-8601 datetime, "
            f"received: {cutoff_time!r}"
        )
    return parsed


def filter_scheduled_matches(
    matches: Iterable[Dict[str, Any]],
    *,
    cutoff: datetime,
    include_completed: bool,
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for match in matches:
        scheduled = parse_match_time(match.get("scheduled_time"))
        if scheduled is None:
            continue
        if scheduled < cutoff:
            continue
        if not include_completed and match.get("match_status") == "F":
            continue
        filtered.append(match)
    return filtered
