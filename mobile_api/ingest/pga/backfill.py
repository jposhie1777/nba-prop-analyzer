from __future__ import annotations

import json
import sys
from pathlib import Path

# Allow running as a script: python mobile_api/ingest/pga/backfill.py
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from mobile_api.ingest.pga.website_ingest import run_website_backfill


def main() -> None:
    summary = run_website_backfill()
    print("PGA website backfill complete.")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
