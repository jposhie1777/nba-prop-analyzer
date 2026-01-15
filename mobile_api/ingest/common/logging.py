from datetime import datetime, timezone

def now_ts():
    return datetime.now(timezone.utc)