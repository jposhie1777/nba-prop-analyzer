from __future__ import annotations

import json
import os
from datetime import datetime, timezone

try:
    from .ingest import run_full_ingestion
except ImportError:
    from mobile_api.ingest.mls.ingest import run_full_ingestion


def _current_season() -> int:
    value = os.getenv("MLS_CURRENT_SEASON")
    if value:
        return int(value)
    return datetime.now(timezone.utc).year


def main() -> None:
    current_season = _current_season()
    result = run_full_ingestion(current_season=current_season)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
