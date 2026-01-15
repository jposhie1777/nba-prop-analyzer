# mobile_api/ingest/common/logging.py
from datetime import datetime, timezone

def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()