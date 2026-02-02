"""
Live ingest runner job.

Runs ingest cycles on a fixed interval for a bounded duration.
Exits early if there are no live games (or pre-game window) to ingest.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Dict

from stater_game_orchestrator import (
    NBA_TZ,
    UTC_TZ,
    PRE_GAME_LEAD_MINUTES,
    fetch_games_from_api,
    fetch_live_box_scores,
)
from managed_live_ingest import INGEST_INTERVAL_SEC, run_full_ingest_cycle


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        return default


JOB_MAX_MINUTES = _read_int_env("LIVE_INGEST_JOB_MAX_MINUTES", 10)


def _parse_start_time_est(game: Dict, game_date: str) -> datetime:
    start_time_str = game.get("datetime") or game.get("start_time")
    if start_time_str:
        start_time_utc = datetime.fromisoformat(
            start_time_str.replace("Z", "+00:00")
        ).astimezone(UTC_TZ)
    else:
        start_time_utc = datetime.fromisoformat(
            f"{game_date}T12:00:00-05:00"
        ).astimezone(UTC_TZ)
    return start_time_utc.astimezone(NBA_TZ)


def _has_live_games() -> bool:
    return bool(fetch_live_box_scores())


def _in_pregame_window(now: datetime) -> bool:
    if PRE_GAME_LEAD_MINUTES <= 0:
        return False

    date_strs = {
        now.date().isoformat(),
        (now + timedelta(days=1)).date().isoformat(),
    }

    for date_str in date_strs:
        games = fetch_games_from_api(date_str)
        for game in games:
            start_time_est = _parse_start_time_est(game, date_str)
            if start_time_est <= now:
                continue
            if start_time_est - timedelta(minutes=PRE_GAME_LEAD_MINUTES) <= now:
                return True

    return False


def _ingest_window_active(now: datetime) -> bool:
    if _has_live_games():
        return True
    return _in_pregame_window(now)


def main() -> None:
    start = datetime.now(NBA_TZ)
    end = start + timedelta(minutes=JOB_MAX_MINUTES)

    print("\n" + "=" * 60)
    print("[INGEST_JOB] Live ingest runner starting")
    print(f"[INGEST_JOB] Start time: {start.isoformat()}")
    print(f"[INGEST_JOB] Max runtime: {JOB_MAX_MINUTES} minutes")
    print(f"[INGEST_JOB] Interval: {INGEST_INTERVAL_SEC} seconds")
    print(f"[INGEST_JOB] Pre-game lead: {PRE_GAME_LEAD_MINUTES} minutes")
    print("=" * 60 + "\n")

    cycles = 0
    while datetime.now(NBA_TZ) < end:
        now = datetime.now(NBA_TZ)
        if not _ingest_window_active(now):
            if cycles == 0:
                print("[INGEST_JOB] No live/pregame window. Exiting.")
            else:
                print("[INGEST_JOB] Live window ended. Exiting.")
            break

        cycle_start = datetime.now(NBA_TZ)
        print(f"\n[INGEST_JOB] Cycle {cycles + 1} @ {cycle_start.strftime('%I:%M:%S %p ET')}")
        run_full_ingest_cycle()
        cycles += 1

        elapsed = (datetime.now(NBA_TZ) - cycle_start).total_seconds()
        sleep_time = max(0, INGEST_INTERVAL_SEC - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

    print(f"[INGEST_JOB] Finished after {cycles} cycle(s).")


if __name__ == "__main__":
    main()
