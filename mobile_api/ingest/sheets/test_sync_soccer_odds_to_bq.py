from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().with_name("sync_soccer_odds_to_bq.py")
_SPEC = importlib.util.spec_from_file_location("sync_soccer_odds_to_bq", _MODULE_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load module from {_MODULE_PATH}")
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

_extract_rows = _MODULE._extract_rows
_resolve_columns = _MODULE._resolve_columns
_league_from_tab = _MODULE._league_from_tab


class SoccerOddsSyncTests(unittest.TestCase):
    def test_league_from_tab(self) -> None:
        self.assertEqual(_league_from_tab("EPL Odds"), "EPL")
        self.assertEqual(_league_from_tab("La Liga Odds"), "LaLiga")
        self.assertEqual(_league_from_tab("MLS Odds"), "MLS")

    def test_resolve_columns_accepts_expected_headers(self) -> None:
        header = ["Game", "Start Time (ET)", "Home", "Away", "Bookmaker", "Market", "Outcome", "Line", "Price"]
        col_map = _resolve_columns(header)
        self.assertEqual(col_map["start_time_et"], 1)
        self.assertEqual(col_map["price"], 8)

    def test_extract_rows_parses_row_and_adds_metadata(self) -> None:
        rows = [
            ["Game", "Start Time (ET)", "Home", "Away", "Bookmaker", "Market", "Outcome", "Line", "Price"],
            [
                "Arsenal @ Wolverhampton Wanderers",
                "02/18/2026 15:00",
                "Wolverhampton Wanderers",
                "Arsenal",
                "FanDuel",
                "btts",
                "Yes",
                "",
                "122",
            ],
        ]

        parsed = _extract_rows("EPL Odds", rows, "2026-02-18 12:00:00")

        self.assertEqual(len(parsed), 1)
        first = parsed[0]
        self.assertEqual(first["league"], "EPL")
        self.assertEqual(first["bookmaker"], "FanDuel")
        self.assertEqual(first["line"], None)
        self.assertEqual(first["price"], 122)


if __name__ == "__main__":
    unittest.main()
