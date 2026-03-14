#!/usr/bin/env python3
"""Parse saved Oddspedia MLS capture files into structured data."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

MARKET_FILES = {
    "1x2": "1x2",
    "btts": "btts",
    "draw_no_bet": "draw_no_bet",
    "double_chance": "double_chance",
    "european_handicap": "euro_handicap",
    "total_corners": "total_corners",
}


def _extract_json_payload(text: str) -> dict[str, Any]:
    marker = re.search(r"\nResponse\n|\nRESPONSE\n", text)
    if marker:
        start = text.find("{", marker.end())
    else:
        start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found")
    return json.loads(text[start:])


def _iter_market_rows(market_name: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    data = payload.get("data", {})
    outcome_names = data.get("outcome_names", [])
    periods_by_id = {str(p.get("id")): p.get("name") for p in payload.get("periods", [])}

    for period_id, period_data in data.get("odds", {}).items():
        # Non-handicap style: {"o1": {...}, "o2": {...}}
        if isinstance(period_data, dict) and "odds" in period_data and isinstance(period_data.get("odds"), dict):
            entries = [(None, period_data)]
        # Handicap style: {"main": {...}, "alternative": [{...}]}
        elif isinstance(period_data, dict) and "main" in period_data:
            entries = [("main", period_data.get("main"))]
            entries.extend(("alternative", alt) for alt in period_data.get("alternative", []))
        else:
            continue

        for line_type, entry in entries:
            if not entry:
                continue
            line_name = entry.get("name")
            for outcome_key, odd_obj in (entry.get("odds") or {}).items():
                outcome_idx = int(outcome_key.removeprefix("o")) - 1
                outcome_name = outcome_names[outcome_idx] if 0 <= outcome_idx < len(outcome_names) else outcome_key
                rows.append(
                    {
                        "market": market_name,
                        "period_id": period_id,
                        "period_name": periods_by_id.get(str(period_id)),
                        "line_type": line_type,
                        "line_name": line_name,
                        "outcome": outcome_name,
                        "bookmaker": odd_obj.get("bookie_name"),
                        "odds": odd_obj.get("odds_value"),
                        "odds_direction": odd_obj.get("odds_direction"),
                        "offer_id": odd_obj.get("offer_id"),
                    }
                )
    return rows


def _extract_statistics_tokens(text: str) -> list[str]:
    marker = text.find("Response")
    if marker == -1:
        return []
    array_start = text.find("[", marker)
    array_end = text.rfind("]")
    if array_start == -1 or array_end == -1 or array_end <= array_start:
        return []
    block = text[array_start : array_end + 1]
    try:
        values = json.loads(block)
        return [v for v in values if isinstance(v, str)]
    except json.JSONDecodeError:
        return []


def build_report(input_dir: Path) -> dict[str, Any]:
    markets: dict[str, Any] = {}
    all_rows: list[dict[str, Any]] = []

    for market_name, file_name in MARKET_FILES.items():
        payload = _extract_json_payload((input_dir / file_name).read_text(encoding="utf-8"))
        rows = _iter_market_rows(market_name, payload)
        all_rows.extend(rows)
        markets[market_name] = {
            "market_name": payload.get("data", {}).get("market_name"),
            "periods": payload.get("periods", []),
            "rows": len(rows),
        }

    match_info = _extract_json_payload((input_dir / "match_info").read_text(encoding="utf-8"))
    match_data = match_info.get("data", {})
    market_keys = [mk.get("statement") for mk in match_data.get("match_keys", []) if mk.get("statement")]

    stats_tokens = _extract_statistics_tokens((input_dir / "statistics_extract").read_text(encoding="utf-8"))

    return {
        "source_dir": str(input_dir),
        "scrapeable_markets": markets,
        "odds_rows": all_rows,
        "match_info": {
            "top_level_fields": sorted(match_data.keys()),
            "match_key": match_data.get("match_key"),
            "teams": {"home": match_data.get("ht"), "away": match_data.get("at")},
            "league": match_data.get("league_name"),
            "venue": match_data.get("venue_name"),
            "market_keys": market_keys,
        },
        "statistics_extract": {
            "token_count": len(stats_tokens),
            "sample": stats_tokens[:80],
            "available": len(stats_tokens) > 0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("website_responses/mls"))
    parser.add_argument("--output", type=Path, default=Path("website_responses/mls/extracted_report.json"))
    args = parser.parse_args()

    report = build_report(args.input_dir)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Wrote report to {args.output}")
    print("Markets:")
    for key, data in report["scrapeable_markets"].items():
        print(f"  - {key}: {data['rows']} normalized odds rows")
    print(f"Match info fields captured: {len(report['match_info']['top_level_fields'])}")
    print(f"Market keys captured: {len(report['match_info']['market_keys'])}")
    print(
        "Statistics extract tokens:",
        report["statistics_extract"]["token_count"],
        "(available=" + str(report["statistics_extract"]["available"]) + ")",
    )


if __name__ == "__main__":
    main()
