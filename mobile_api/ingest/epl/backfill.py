from __future__ import annotations

import json
import os
import argparse
from datetime import datetime, timezone

try:
    from .ingest import run_backfill, run_full_ingestion
except ImportError:
    from mobile_api.ingest.epl.ingest import run_backfill, run_full_ingestion


def _current_season() -> int:
    value = os.getenv("EPL_CURRENT_SEASON")
    if value:
        return int(value)
    return datetime.now(timezone.utc).year


def main() -> None:
    parser = argparse.ArgumentParser(description="EPL ingestion runner")
    parser.add_argument("--start-season", type=int, default=None)
    parser.add_argument("--end-season", type=int, default=None)
    parser.add_argument("--truncate-first", action=argparse.BooleanOptionalAction, default=False)
    args = parser.parse_args()

    if args.start_season is not None or args.end_season is not None:
        end_season = args.end_season if args.end_season is not None else _current_season()
        start_season = args.start_season if args.start_season is not None else end_season
        result = run_backfill(
            start_season=start_season,
            end_season=end_season,
            truncate_first=args.truncate_first,
        )
    elif args.truncate_first:
        # For workflow usage where no explicit season range is passed,
        # truncate/rebuild should still run the standard previous+current window.
        current_season = _current_season()
        result = run_backfill(
            start_season=current_season - 1,
            end_season=current_season,
            truncate_first=True,
        )
    else:
        current_season = _current_season()
        result = run_full_ingestion(current_season=current_season)

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
