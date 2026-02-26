"""
PGA Tour pairings ingest Cloud Run Job.

Determines which rounds to fetch based on the current day of week (ET),
calls the PGA Tour GraphQL API, and writes pairings to BigQuery.

Round publication schedule
--------------------------
  Wednesday        R1 + R2 tee times are published (typically noon–6 pm ET)
  Thursday         R1 in play  – safety re-check R1 at 8 am
  Friday           R2 in play  – safety re-check R2 at 8 am
  Saturday         R3 published after R2 finishes (~3–6 pm ET)
  Sunday           R4 published after R3 finishes (~3–7 pm ET)
  Monday/Tuesday   No tournament activity — job exits immediately

Cloud Scheduler schedules (all America/New_York)
-------------------------------------------------
  Wed  10 am – 11 pm  hourly      : 0 10-23 * * 3
  Thu  8 am            once       : 0 8 * * 4
  Fri  8 am            once       : 0 8 * * 5
  Sat  2 pm –  9 pm   every 30 min: */30 14-21 * * 6
  Sun  2 pm –  9 pm   every 30 min: */30 14-21 * * 0

Required env vars
-----------------
  GCP_PROJECT                – GCP project (used by BigQuery client)
  PGA_CURRENT_TOURNAMENT_ID  – Tournament ID, e.g. "R2026010"
                               Update this at the start of each week.

Optional env vars
-----------------
  PGA_DATASET        – BigQuery dataset  (default: pga_data)
  PGA_PAIRINGS_TABLE – BigQuery table    (default: tournament_round_pairings)
  PGA_DRY_RUN        – Set to "true" to fetch without writing to BigQuery
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

# Add the mobile_api directory to sys.path when running as a Cloud Run job
# (python -m jobs.pga_pairings_job from /app or wherever the image places code).
import pathlib
_repo = pathlib.Path(__file__).resolve().parent.parent
if str(_repo) not in sys.path:
    sys.path.insert(0, str(_repo))

from ingest.pga.pga_pairings_ingest import ingest_pairings  # noqa: E402

ET = ZoneInfo("America/New_York")

# Rounds to check per day-of-week  (ISO weekday: 1=Mon … 7=Sun)
ROUNDS_BY_DOW: dict[int, list[int]] = {
    3: [1, 2],   # Wednesday  — R1 + R2 publication day
    4: [1],      # Thursday   — R1 safety refresh
    5: [2],      # Friday     — R2 safety refresh
    6: [3],      # Saturday   — R3 published after R2 ends
    7: [4],      # Sunday     — R4 published after R3 ends
}


def _get_tournament_id() -> str:
    tid = os.getenv("PGA_CURRENT_TOURNAMENT_ID", "").strip()
    if not tid:
        raise RuntimeError(
            "PGA_CURRENT_TOURNAMENT_ID is not set. "
            "Set it to the current tournament ID, e.g. 'R2026010'. "
            "Find it in the URL on pgatour.com."
        )
    return tid


def _dry_run() -> bool:
    return os.getenv("PGA_DRY_RUN", "false").strip().lower() == "true"


def main() -> None:
    now = datetime.now(ET)
    dow = now.isoweekday()
    dow_name = now.strftime("%A")

    print("=" * 60)
    print("[PGA_PAIRINGS_JOB] Starting")
    print(f"[PGA_PAIRINGS_JOB] Time      : {now.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"[PGA_PAIRINGS_JOB] Day       : {dow_name}")

    rounds = ROUNDS_BY_DOW.get(dow, [])
    if not rounds:
        print(f"[PGA_PAIRINGS_JOB] No rounds scheduled for {dow_name}. Exiting.")
        print("=" * 60)
        return

    tournament_id = _get_tournament_id()
    dry = _dry_run()

    print(f"[PGA_PAIRINGS_JOB] Tournament: {tournament_id}")
    print(f"[PGA_PAIRINGS_JOB] Rounds    : {rounds}")
    print(f"[PGA_PAIRINGS_JOB] Dry run   : {dry}")
    print("=" * 60)

    results = []
    for rnd in rounds:
        print(f"\n[PGA_PAIRINGS_JOB] ── Round {rnd} ──")
        try:
            summary = ingest_pairings(
                tournament_id=tournament_id,
                round_number=rnd,
                create_tables=True,
                dry_run=dry,
            )
            results.append(summary)

            if summary["groups"] == 0:
                print(
                    f"[PGA_PAIRINGS_JOB]   Round {rnd} tee times not yet published "
                    f"— will retry on next scheduled run."
                )
            elif dry:
                print(
                    f"[PGA_PAIRINGS_JOB]   DRY RUN: found {summary['groups']} groups "
                    f"/ {summary['player_rows']} player rows (nothing written to BQ)."
                )
            else:
                print(
                    f"[PGA_PAIRINGS_JOB]   Inserted {summary['inserted']} rows "
                    f"({summary['groups']} groups, {summary['player_rows']} players)."
                )

        except Exception as exc:
            print(f"[PGA_PAIRINGS_JOB]   ERROR: {exc}")
            results.append({"round_number": rnd, "error": str(exc)})

    print("\n" + "=" * 60)
    errors = [r for r in results if "error" in r]
    if errors:
        print(f"[PGA_PAIRINGS_JOB] Completed with {len(errors)} error(s). Exiting 1.")
        print("=" * 60)
        sys.exit(1)

    total_inserted = sum(r.get("inserted", 0) for r in results)
    total_groups = sum(r.get("groups", 0) for r in results)
    print(
        f"[PGA_PAIRINGS_JOB] Done. "
        f"Rounds checked: {len(rounds)}  "
        f"Groups: {total_groups}  "
        f"Rows inserted: {total_inserted}"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
