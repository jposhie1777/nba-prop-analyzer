from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().with_name("schedule_filter.py")
_SPEC = importlib.util.spec_from_file_location("schedule_filter", _MODULE_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load schedule_filter module from {_MODULE_PATH}")
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

filter_scheduled_matches = _MODULE.filter_scheduled_matches
parse_match_time = _MODULE.parse_match_time
resolve_cutoff_time = _MODULE.resolve_cutoff_time


class ScheduleFilterTests(unittest.TestCase):
    def test_parse_match_time_normalizes_to_utc(self) -> None:
        parsed = parse_match_time("2026-02-11T13:30:00Z")
        self.assertEqual(parsed, datetime(2026, 2, 11, 13, 30, tzinfo=timezone.utc))

    def test_resolve_cutoff_defaults_to_start_of_utc_day(self) -> None:
        now = datetime(2026, 2, 11, 18, 45, tzinfo=timezone.utc)
        cutoff = resolve_cutoff_time(None, now=now)
        self.assertEqual(cutoff, datetime(2026, 2, 11, 0, 0, tzinfo=timezone.utc))

    def test_filter_scheduled_matches_keeps_earlier_today_matches(self) -> None:
        matches = [
            {"id": 1, "scheduled_time": "2026-02-11T08:00:00Z", "match_status": "scheduled"},
            {"id": 2, "scheduled_time": "2026-02-11T10:00:00Z", "match_status": "scheduled"},
            {"id": 3, "scheduled_time": "2026-02-11T14:00:00Z", "match_status": "scheduled"},
            {"id": 4, "scheduled_time": "2026-02-11T16:00:00Z", "match_status": "scheduled"},
        ]
        from_now = datetime(2026, 2, 11, 12, 0, tzinfo=timezone.utc)
        from_day_start = datetime(2026, 2, 11, 0, 0, tzinfo=timezone.utc)

        filtered_from_now = filter_scheduled_matches(
            matches,
            cutoff=from_now,
            include_completed=False,
        )
        filtered_from_day_start = filter_scheduled_matches(
            matches,
            cutoff=from_day_start,
            include_completed=False,
        )

        self.assertEqual(len(filtered_from_now), 2)
        self.assertEqual(len(filtered_from_day_start), 4)

    def test_filter_excludes_completed_when_flag_disabled(self) -> None:
        matches = [
            {"id": 1, "scheduled_time": "2026-02-11T13:30:00Z", "match_status": "F"},
            {"id": 2, "scheduled_time": "2026-02-11T15:00:00Z", "match_status": "in_progress"},
        ]
        cutoff = datetime(2026, 2, 11, 0, 0, tzinfo=timezone.utc)

        filtered_default = filter_scheduled_matches(
            matches,
            cutoff=cutoff,
            include_completed=False,
        )
        filtered_with_completed = filter_scheduled_matches(
            matches,
            cutoff=cutoff,
            include_completed=True,
        )

        self.assertEqual([match["id"] for match in filtered_default], [2])
        self.assertEqual([match["id"] for match in filtered_with_completed], [1, 2])

    def test_invalid_cutoff_time_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            resolve_cutoff_time("not-a-datetime")


if __name__ == "__main__":
    unittest.main()
