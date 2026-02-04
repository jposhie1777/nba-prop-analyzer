"""
Daily schedule refresh job.

Fetches today's games from BallDontLie and writes schedule to BigQuery.
Intended to run once daily around 5:30-6:00 AM ET via Cloud Scheduler.
"""

from __future__ import annotations

from datetime import datetime

from managed_live_ingest import nba_today
from stater_game_orchestrator import NBA_TZ, fetch_and_write_daily_schedule
from three_q_100 import refresh_three_q_100_predictions


def main() -> None:
    today = nba_today().isoformat()
    now = datetime.now(NBA_TZ)

    print("\n" + "=" * 60)
    print("[SCHEDULE_REFRESH] Starting")
    print(f"[SCHEDULE_REFRESH] Time: {now.isoformat()}")
    print(f"[SCHEDULE_REFRESH] NBA Today: {today}")
    print("=" * 60 + "\n")

    session = fetch_and_write_daily_schedule(today)
    print(f"[SCHEDULE_REFRESH] Games written: {len(session.games)}")

    try:
        refresh_result = refresh_three_q_100_predictions(today)
        print(
            "[SCHEDULE_REFRESH] 3Q-100 table refreshed:"
            f" {refresh_result.get('rows', 0)} rows"
        )
    except Exception as exc:
        print(f"[SCHEDULE_REFRESH] 3Q-100 refresh failed: {exc}")


if __name__ == "__main__":
    main()
