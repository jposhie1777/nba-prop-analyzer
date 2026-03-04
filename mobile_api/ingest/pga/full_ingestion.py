from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Allow running as a script: python mobile_api/ingest/pga/full_ingestion.py
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from mobile_api.ingest.pga.website_ingest import run_website_ingestion


def main() -> None:
    season_env = os.getenv("PGA_SEASON")
    season = int(season_env) if season_env else None
    summary = run_website_ingestion(
        season=season,
        create_tables=True,
        truncate_first=False,
    )
    print("PGA website full ingestion complete.")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
