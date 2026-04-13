#propfinder/run_workflow.py

"""
Cron-friendly PropFinder workflow runner.

Runs the full MLB pipeline in a single command:
  1) Optional schema setup
  2) ingest.py
  3) model.py

Designed for repeated scheduled execution with:
- clear step logging
- non-zero exit codes on failure
- optional lock file to prevent overlapping cron runs
"""

from __future__ import annotations

import argparse
import fcntl
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple


BASE_DIR = Path(__file__).resolve().parent


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(message: str) -> None:
    print(f"[{_ts()}] {message}", flush=True)


def _run_step(step_name: str, script_name: str) -> int:
    script_path = BASE_DIR / script_name
    if not script_path.exists():
        _log(f"ERROR: step '{step_name}' script not found: {script_path}")
        return 1

    start = time.time()
    cmd = [sys.executable, str(script_path)]
    _log(f"START {step_name}: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(BASE_DIR))
    elapsed = time.time() - start

    if result.returncode != 0:
        _log(f"FAIL  {step_name}: exit={result.returncode} elapsed={elapsed:.1f}s")
        return result.returncode

    _log(f"DONE  {step_name}: elapsed={elapsed:.1f}s")
    return 0


def _acquire_lock(lock_file: Path, on_lock: str) -> Tuple[int, object | None]:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_file, "w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        if on_lock == "skip":
            _log(f"SKIP: lock is already held: {lock_file}")
            handle.close()
            return 0, None
        _log(f"FAIL: lock is already held: {lock_file}")
        handle.close()
        return 2, None

    handle.write(str(os.getpid()))
    handle.flush()
    _log(f"LOCK acquired: {lock_file}")
    return 0, handle


def _steps(args: argparse.Namespace) -> Iterable[Tuple[str, str]]:
    if args.setup:
        yield "setup_bq", "setup_bq.py"
    if not args.skip_ingest:
        yield "ingest", "ingest.py"
    if not args.skip_model:
        yield "model", "model.py"
    if not args.skip_alerts:
        yield "discord_alerts", "discord_alerts.py"
    if not args.skip_analytics:
        yield "analytics", "analytics.py"
    # K prop pipeline
    if not args.skip_fd_k_scraper:
        yield "fd_k_scraper", "fd_k_scraper.py"
    if not args.skip_k_model:
        yield "k_model", "k_model.py"
    if not args.skip_k_alerts:
        yield "k_discord_alerts", "k_discord_alerts.py"
    if not args.skip_k_analytics:
        yield "k_analytics", "k_analytics.py"


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PropFinder workflow.")
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Run setup_bq.py before ingest/model.",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip ingest.py.",
    )
    parser.add_argument(
        "--skip-model",
        action="store_true",
        help="Skip model.py.",
    )
    parser.add_argument(
        "--skip-alerts",
        action="store_true",
        help="Skip discord_alerts.py.",
    )
    parser.add_argument(
        "--skip-analytics",
        action="store_true",
        help="Skip analytics.py.",
    )
    parser.add_argument(
        "--skip-fd-k-scraper",
        action="store_true",
        help="Skip fd_k_scraper.py (FanDuel K under lines).",
    )
    parser.add_argument(
        "--skip-k-model",
        action="store_true",
        help="Skip k_model.py.",
    )
    parser.add_argument(
        "--skip-k-alerts",
        action="store_true",
        help="Skip k_discord_alerts.py.",
    )
    parser.add_argument(
        "--skip-k-analytics",
        action="store_true",
        help="Skip k_analytics.py.",
    )
    parser.add_argument(
        "--lock-file",
        default=os.getenv("PROPFINDER_LOCK_FILE", "/tmp/propfinder_workflow.lock"),
        help="Path for lock file used to avoid overlapping runs.",
    )
    parser.add_argument(
        "--on-lock",
        choices=("skip", "fail"),
        default="skip",
        help="Behavior when another workflow run is already in progress.",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    lock_path = Path(args.lock_file)

    _log("PropFinder workflow starting")
    _log(f"cwd={BASE_DIR}")

    lock_status, lock_handle = _acquire_lock(lock_path, args.on_lock)
    if lock_status != 0 or lock_handle is None:
        return lock_status

    started = time.time()
    try:
        for step_name, script_name in _steps(args):
            code = _run_step(step_name, script_name)
            if code != 0:
                _log("PropFinder workflow failed")
                return code
    finally:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            lock_handle.close()
        _log(f"LOCK released: {lock_path}")

    _log(f"PropFinder workflow complete in {time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
