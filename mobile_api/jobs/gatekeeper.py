"""
Gatekeeper job that triggers live ingest only during live games.

This job is meant to be run by Cloud Scheduler on a fixed cadence.
It checks for live games (or optional pre-game window) and, if active,
starts a Cloud Run Job execution for the ingest runner.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict

import google.auth
from google.auth.transport.requests import AuthorizedSession

from stater_game_orchestrator import (
    NBA_TZ,
    UTC_TZ,
    PRE_GAME_LEAD_MINUTES,
    fetch_games_from_api,
    fetch_live_box_scores,
)


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _get_project() -> str:
    return os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT") or ""


def _parse_start_time_est(game: Dict, game_date: str) -> datetime:
    start_time_str = game.get("datetime") or game.get("start_time")
    if start_time_str:
        start_time_utc = datetime.fromisoformat(
            start_time_str.replace("Z", "+00:00")
        ).astimezone(UTC_TZ)
    else:
        # Fallback: use date at noon
        start_time_utc = datetime.fromisoformat(
            f"{game_date}T12:00:00-05:00"
        ).astimezone(UTC_TZ)
    return start_time_utc.astimezone(NBA_TZ)


def _has_live_games() -> bool:
    live_games = fetch_live_box_scores()
    return bool(live_games)


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


def _has_running_execution(session: AuthorizedSession, base_url: str) -> bool:
    resp = session.get(f"{base_url}/executions", params={"pageSize": 20})
    resp.raise_for_status()
    data = resp.json()
    executions = data.get("executions", [])
    for execution in executions:
        state = (execution.get("state") or "").upper()
        if state in ("RUNNING", "RECONCILING"):
            return True
    return False


def _run_ingest_job(session: AuthorizedSession, base_url: str) -> None:
    resp = session.post(f"{base_url}:run", json={})
    resp.raise_for_status()
    payload = resp.json()
    print(f"[GATEKEEPER] Triggered execution: {payload.get('name')}")


def main() -> None:
    project = _get_project()
    if not project:
        raise RuntimeError("Missing required env var: GCP_PROJECT or GOOGLE_CLOUD_PROJECT")
    region = _get_env("CLOUD_RUN_REGION")
    ingest_job = _get_env("CLOUD_RUN_INGEST_JOB")

    now = datetime.now(NBA_TZ)
    print(f"[GATEKEEPER] Check @ {now.isoformat()}")
    print(f"[GATEKEEPER] Pre-game lead minutes: {PRE_GAME_LEAD_MINUTES}")

    if _has_live_games():
        reason = "live_games"
    elif _in_pregame_window(now):
        reason = "pregame_window"
    else:
        print("[GATEKEEPER] No live/pregame window. Exiting.")
        return

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    authed = AuthorizedSession(credentials)
    base_url = (
        f"https://run.googleapis.com/v2/projects/{project}"
        f"/locations/{region}/jobs/{ingest_job}"
    )

    if _has_running_execution(authed, base_url):
        print("[GATEKEEPER] Ingest job already running. Skipping.")
        return

    print(f"[GATEKEEPER] Active window: {reason}. Triggering ingest job...")
    _run_ingest_job(authed, base_url)


if __name__ == "__main__":
    main()
