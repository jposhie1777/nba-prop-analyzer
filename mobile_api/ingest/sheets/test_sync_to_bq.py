from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().with_name("sync_to_bq.py")
_SPEC = importlib.util.spec_from_file_location("sync_to_bq", _MODULE_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load sync_to_bq module from {_MODULE_PATH}")
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

resolve_column_map = _MODULE.resolve_column_map
is_match_on_date = _MODULE.is_match_on_date
COL = _MODULE.COL


class SyncToBqTests(unittest.TestCase):
    def test_resolve_column_map_supports_winner_label_layout(self) -> None:
        header = [
            "Id", "Id", "Name", "Location", "Surface", "Category", "Season", "Start Date", "End Date", "Prize Money",
            "Prize Currency", "Draw Size", "Season", "Round", "Id", "First Name", "Last Name", "Full Name", "Country",
            "Country Code", "Birth Place", "Age", "Height Cm", "Weight Kg", "Plays", "Turned Pro", "Id", "First Name",
            "Last Name", "Full Name", "Country", "Country Code", "Birth Place", "Age", "Height Cm", "Weight Kg", "Plays",
            "Turned Pro", "Winner", "Score", "Duration", "Number Of Sets", "Match Status", "Is Live", "Scheduled Time",
            "Not Before Text", "Id", "First Name", "Last Name", "Full Name", "Country", "Country Code", "Birth Place", "Age",
            "Height Cm", "Weight Kg", "Plays", "Turned Pro",
        ]

        col_map = resolve_column_map(header)

        self.assertEqual(col_map["scheduled_time"], 44)
        self.assertEqual(col_map["match_status"], 42)
        self.assertEqual(col_map["winner_id"], 46)

    def test_is_match_on_date_uses_resolved_index(self) -> None:
        header = [""] * 58
        header[38] = "Winner"
        header[44] = "Scheduled Time"
        col_map = resolve_column_map(header)

        row = [""] * 58
        row[44] = "2026-02-16T18:00:00.000Z"

        self.assertTrue(is_match_on_date(row, "2026-02-16", col_map))

    def test_default_layout_still_works(self) -> None:
        header = [""] * 58
        header[38] = "Id"
        col_map = resolve_column_map(header)
        self.assertEqual(col_map["scheduled_time"], COL["scheduled_time"])


if __name__ == "__main__":
    unittest.main()
