"""generate_capture_urls.py

Reads the saved Oddspedia bulk set_odds capture and outputs the per-match
getMatchMaxOddsByGroup API URLs for every matchId × marketGroupId combination.

These URLs can be:
  - Pasted into the browser (Network tab) to capture the responses
  - Fetched programmatically with requests/curl

Usage
-----
    python -m mobile_api.ingest.atp.generate_capture_urls

    # Point at a different set_odds file:
    python -m mobile_api.ingest.atp.generate_capture_urls \
        --set-odds website_responses/atp/set_odds

    # Only generate URLs for specific market groups:
    python -m mobile_api.ingest.atp.generate_capture_urls \
        --market-groups 201 301 401

Output format (one URL per line):
    # matchId=9980599  marketGroupId=201  (Moneyline)
    https://oddspedia.com/api/v1/getMatchMaxOddsByGroup?matchId=9980599&inplay=0&marketGroupId=201&geoCode=US&geoState=NY&language=us

    # matchId=9980599  marketGroupId=301  (Spread)
    https://oddspedia.com/api/v1/getMatchMaxOddsByGroup?matchId=9980599&inplay=0&marketGroupId=301&geoCode=US&geoState=NY&language=us
    ...

Suggested file naming for website_responses/atp/match_specific/:
    {matchId}_{marketGroupId}
    e.g.  9980599_201   9980599_301
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlencode

# Market group IDs supported for ATP tennis
MARKET_GROUPS: dict[int, str] = {
    201: "Moneyline",
    301: "Spread / Set Handicap",
}

BASE_API = "https://oddspedia.com/api/v1/getMatchMaxOddsByGroup"

COMMON_PARAMS = {
    "inplay": "0",
    "geoCode": "US",
    "geoState": "NY",
    "language": "us",
}


def _extract_json(text: str) -> dict:
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON found in file")
    return json.loads(text[start:])


def _load_match_ids(set_odds_path: Path) -> list[str]:
    text = set_odds_path.read_text(encoding="utf-8")
    data = _extract_json(text)
    matches = data.get("data", {}).get("matches", {})
    return list(matches.keys())


def _make_url(match_id: str, market_group_id: int) -> str:
    params = {"matchId": match_id, "marketGroupId": str(market_group_id), **COMMON_PARAMS}
    return f"{BASE_API}?{urlencode(params)}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate per-match Oddspedia capture URLs from set_odds file")
    parser.add_argument(
        "--set-odds",
        type=Path,
        default=Path("website_responses/atp/set_odds"),
        help="Path to the bulk set_odds capture file",
    )
    parser.add_argument(
        "--market-groups",
        type=int,
        nargs="+",
        default=list(MARKET_GROUPS.keys()),
        help="Market group IDs to generate URLs for (default: 201 301)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write URLs to a file instead of stdout",
    )
    args = parser.parse_args()

    match_ids = _load_match_ids(args.set_odds)
    print(f"Found {len(match_ids)} match IDs in {args.set_odds}", flush=True)

    lines: list[str] = []
    lines.append(f"# Generated from {args.set_odds}")
    lines.append(f"# {len(match_ids)} matches × {len(args.market_groups)} market groups = {len(match_ids) * len(args.market_groups)} URLs")
    lines.append(f"#")
    lines.append(f"# Save each response as:  website_responses/atp/match_specific/{{matchId}}_{{marketGroupId}}")
    lines.append("")

    for match_id in match_ids:
        for mg_id in args.market_groups:
            label = MARKET_GROUPS.get(mg_id, f"market_group_{mg_id}")
            lines.append(f"# matchId={match_id}  marketGroupId={mg_id}  ({label})")
            lines.append(_make_url(match_id, mg_id))
            lines.append("")

    output_text = "\n".join(lines)

    if args.output:
        args.output.write_text(output_text, encoding="utf-8")
        print(f"URLs written to {args.output}")
    else:
        print(output_text)


if __name__ == "__main__":
    main()
