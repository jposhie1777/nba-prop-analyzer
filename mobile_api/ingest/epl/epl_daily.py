#/workspaces/nba-prop-analyzer/mobile_api/ingest/epl/epl_daily.py
from __future__ import annotations

import argparse
import json

try:
    from .ingest import run_daily_ingest
except ImportError:
    from mobile_api.ingest.epl.ingest import run_daily_ingest


def main() -> None:
    parser = argparse.ArgumentParser(description="EPL daily ingest runner")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to ingest (YYYY-MM-DD). Defaults to yesterday UTC.",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Override current season year (YYYY).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Fetch and preview data but do NOT write to BigQuery.",
    )
    args = parser.parse_args()

    result = run_daily_ingest(
        target_date=args.date,
        current_season=args.season,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
