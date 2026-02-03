from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional


CUT_MARKERS = {"CUT", "MC", "WD", "DQ", "DNS"}


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_yardage(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None


def is_cut(position: Optional[str]) -> bool:
    if not position:
        return False
    pos = position.upper().strip()
    return pos in CUT_MARKERS or "CUT" in pos


def finish_value(position_numeric: Optional[int], position: Optional[str], cut_penalty: int) -> int:
    if position_numeric is not None:
        return position_numeric
    if is_cut(position):
        return cut_penalty
    return cut_penalty


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def serialize_datetime_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    serialized: Dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value
    return serialized
