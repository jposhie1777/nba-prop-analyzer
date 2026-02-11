from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


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


def default_utc_day_cutoff(now: Optional[datetime] = None) -> datetime:
    current = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
    return datetime(current.year, current.month, current.day, tzinfo=timezone.utc)


def resolve_cutoff_time(cutoff_time: Optional[str], *, now: Optional[datetime] = None) -> datetime:
    if cutoff_time is None:
        return default_utc_day_cutoff(now)
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
