"""
ATP Hawkeye Match Stats Ingest

Fetches per-match stats from the ATP Hawkeye API for all matches in
website_match_results for the requested year range, and writes to
website_hawkeye_match_stats in BigQuery.

API endpoint (no auth required):
  https://www.atptour.com/-/Hawkeye/MatchStats/Complete/{year}/{tournament_id}/{match_id}

Stats are stored one row per player per set (SetNumber=0 = match totals).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from google.api_core import exceptions as gcp_exceptions
from google.api_core.retry import Retry
from google.cloud import bigquery


# ------------------------------------------------------------------ #
# BQ helpers                                                           #
# ------------------------------------------------------------------ #

def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _dataset() -> str:
    return os.getenv("ATP_DATASET", "atp_data")


def _table(name: str) -> str:
    return f"{_dataset()}.{name}"


def _chunked(rows: Sequence[Dict], size: int) -> Iterable[Sequence[Dict]]:
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


def _insert_rows(client: bigquery.Client, table_name: str, rows: List[Dict]) -> int:
    if not rows:
        return 0
    batch_size = 200
    inserted = 0
    for batch in _chunked(rows, batch_size):
        errors = client.insert_rows_json(_table(table_name), list(batch), retry=Retry(deadline=60))
        if errors:
            raise RuntimeError(f"BQ insert errors for {table_name}: {errors}")
        inserted += len(batch)
    return inserted


# ------------------------------------------------------------------ #
# HTTP                                                                 #
# ------------------------------------------------------------------ #

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.atptour.com/en/scores/stats-centre/",
}


def _fetch_match_stats(year: int, tournament_id: str, match_id: str, timeout: int = 15) -> Optional[Dict]:
    url = f"https://www.atptour.com/-/Hawkeye/MatchStats/Complete/{year}/{tournament_id}/{match_id}"
    for attempt in range(3):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(10 * (attempt + 1))
                continue
            return None
        except Exception:
            if attempt < 2:
                time.sleep(5)
    return None



# ------------------------------------------------------------------ #
# URL parsing                                                          #
# ------------------------------------------------------------------ #

def _parse_stats_url(stats_url: str) -> Optional[Tuple[int, str, str]]:
    """
    Extract (year, tournament_id, match_id) from a stats_url like:
      /en/scores/stats-centre/archive/2026/404/ms001
    """
    m = re.search(r"/stats-centre/archive/(\d{4})/([^/]+)/([^/?#]+)", stats_url, re.IGNORECASE)
    if not m:
        return None
    return int(m.group(1)), m.group(2), m.group(3)


# ------------------------------------------------------------------ #
# Normalization                                                        #
# ------------------------------------------------------------------ #

def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _extract_stat(block: Dict, key: str, field: str = "Number") -> Optional[int]:
    sub = block.get(key)
    if isinstance(sub, dict):
        return _safe_int(sub.get(field))
    return None


def _extract_pct(block: Dict, key: str) -> Optional[float]:
    sub = block.get(key)
    if isinstance(sub, dict):
        return _safe_float(sub.get("Percent"))
    return None


def _extract_fraction(block: Dict, key: str) -> Tuple[Optional[int], Optional[int]]:
    """Returns (dividend, divisor) — e.g. first serves made / attempted."""
    sub = block.get(key)
    if isinstance(sub, dict):
        return _safe_int(sub.get("Dividend")), _safe_int(sub.get("Divisor"))
    return None, None


def _normalize_player_set(
    snapshot_ts: str,
    ingest_run_id: str,
    stats_url: str,
    year: int,
    tournament_id: str,
    match_id: str,
    match_date: Optional[str],
    tournament_name: Optional[str],
    surface: Optional[str],
    round_name: Optional[str],
    match_duration: Optional[str],
    player_id: Optional[str],
    player_name: Optional[str],
    opponent_id: Optional[str],
    opponent_name: Optional[str],
    is_winner: bool,
    set_number: int,
    set_score: Optional[int],
    opponent_set_score: Optional[int],
    service_stats: Dict,
    return_stats: Dict,
    point_stats: Dict,
) -> Dict:
    fs_made, fs_attempted = _extract_fraction(service_stats, "FirstServe")
    fsw_made, fsw_attempted = _extract_fraction(service_stats, "FirstServePointsWon")
    ssw_made, ssw_attempted = _extract_fraction(service_stats, "SecondServePointsWon")
    bps_saved, bps_faced = _extract_fraction(service_stats, "BreakPointsSaved")
    fsrpw_made, fsrpw_attempted = _extract_fraction(return_stats, "FirstServeReturnPointsWon")
    ssrpw_made, ssrpw_attempted = _extract_fraction(return_stats, "SecondServeReturnPointsWon")
    bpc_made, bpc_attempted = _extract_fraction(return_stats, "BreakPointsConverted")
    tspw_made, tspw_attempted = _extract_fraction(point_stats, "TotalServicePointsWon")
    trpw_made, trpw_attempted = _extract_fraction(point_stats, "TotalReturnPointsWon")
    tpw_made, tpw_attempted = _extract_fraction(point_stats, "TotalPointsWon")

    return {
        "snapshot_ts_utc": snapshot_ts,
        "ingest_run_id": ingest_run_id,
        "stats_url": stats_url,
        "year": year,
        "tournament_id": tournament_id,
        "match_id": match_id,
        "match_date": match_date,
        "tournament_name": tournament_name,
        "surface": surface,
        "round_name": round_name,
        "match_duration": match_duration,
        "player_id": player_id,
        "player_name": player_name,
        "opponent_id": opponent_id,
        "opponent_name": opponent_name,
        "is_winner": is_winner,
        "set_number": set_number,
        "set_score": set_score,
        "opponent_set_score": opponent_set_score,
        # Service
        "aces": _extract_stat(service_stats, "Aces"),
        "double_faults": _extract_stat(service_stats, "DoubleFaults"),
        "serve_rating": _extract_stat(service_stats, "ServeRating"),
        "first_serve_pct": _extract_pct(service_stats, "FirstServe"),
        "first_serve_made": fs_made,
        "first_serve_attempted": fs_attempted,
        "first_serve_pts_won_pct": _extract_pct(service_stats, "FirstServePointsWon"),
        "first_serve_pts_won": fsw_made,
        "first_serve_pts_played": fsw_attempted,
        "second_serve_pts_won_pct": _extract_pct(service_stats, "SecondServePointsWon"),
        "second_serve_pts_won": ssw_made,
        "second_serve_pts_played": ssw_attempted,
        "break_points_saved_pct": _extract_pct(service_stats, "BreakPointsSaved"),
        "break_points_saved": bps_saved,
        "break_points_faced": bps_faced,
        "service_games_played": _extract_stat(service_stats, "ServiceGamesPlayed"),
        # Return
        "return_rating": _extract_stat(return_stats, "ReturnRating"),
        "first_serve_return_pts_won_pct": _extract_pct(return_stats, "FirstServeReturnPointsWon"),
        "first_serve_return_pts_won": fsrpw_made,
        "first_serve_return_pts_played": fsrpw_attempted,
        "second_serve_return_pts_won_pct": _extract_pct(return_stats, "SecondServeReturnPointsWon"),
        "second_serve_return_pts_won": ssrpw_made,
        "second_serve_return_pts_played": ssrpw_attempted,
        "break_points_converted_pct": _extract_pct(return_stats, "BreakPointsConverted"),
        "break_points_converted": bpc_made,
        "break_points_opportunities": bpc_attempted,
        "return_games_played": _extract_stat(return_stats, "ReturnGamesPlayed"),
        # Points
        "total_service_pts_won_pct": _extract_pct(point_stats, "TotalServicePointsWon"),
        "total_service_pts_won": tspw_made,
        "total_service_pts_played": tspw_attempted,
        "total_return_pts_won_pct": _extract_pct(point_stats, "TotalReturnPointsWon"),
        "total_return_pts_won": trpw_made,
        "total_return_pts_played": trpw_attempted,
        "total_pts_won_pct": _extract_pct(point_stats, "TotalPointsWon"),
        "total_pts_won": tpw_made,
        "total_pts_played": tpw_attempted,
    }


def _parse_response(
    data: Dict,
    stats_url: str,
    year: int,
    tournament_id: str,
    match_id: str,
    match_date: Optional[str],
    snapshot_ts: str,
    ingest_run_id: str,
) -> List[Dict]:
    rows: List[Dict] = []

    tournament = data.get("Tournament") or {}
    match = data.get("Match") or {}

    tournament_name = tournament.get("TournamentName") or tournament.get("EventDisplayName")
    surface = tournament.get("Court")
    round_name = (match.get("Round") or {}).get("LongName") or match.get("RoundName")
    match_duration = match.get("MatchTimeTotal") or match.get("MatchTime")
    winning_player_id = match.get("WinningPlayerId") or match.get("Winner")

    def _get_player_info(team: Dict) -> Tuple[Optional[str], Optional[str]]:
        player = team.get("Player") or {}
        pid = player.get("PlayerId")
        fname = player.get("PlayerFirstName") or ""
        lname = player.get("PlayerLastName") or ""
        name = f"{fname} {lname}".strip() or None
        return pid, name

    # Both teams
    player_team = match.get("PlayerTeam") or {}
    opponent_team = match.get("OpponentTeam") or {}

    p1_id, p1_name = _get_player_info(player_team)
    p2_id, p2_name = _get_player_info(opponent_team)

    p1_sets = player_team.get("SetScores") or []
    p2_sets = {s.get("SetNumber"): s for s in (opponent_team.get("SetScores") or [])}

    for set_data in p1_sets:
        set_num = set_data.get("SetNumber", 0)
        p2_set = p2_sets.get(set_num, {})

        stats_block = set_data.get("Stats") or {}
        service = stats_block.get("ServiceStats") or {}
        ret = stats_block.get("ReturnStats") or {}
        pts = stats_block.get("PointStats") or {}

        p2_stats_block = p2_set.get("Stats") or {}
        p2_service = p2_stats_block.get("ServiceStats") or {}
        p2_ret = p2_stats_block.get("ReturnStats") or {}
        p2_pts = p2_stats_block.get("PointStats") or {}


        # Skip sets with no stats
        if not service and not ret and not pts:
            continue

        p1_set_score = _safe_int(set_data.get("SetScore"))
        p2_set_score = _safe_int(p2_set.get("SetScore"))

        rows.append(_normalize_player_set(
            snapshot_ts, ingest_run_id, stats_url, year, tournament_id, match_id,
            match_date, tournament_name, surface, round_name, match_duration,
            p1_id, p1_name, p2_id, p2_name,
            is_winner=(p1_id == winning_player_id),
            set_number=set_num,
            set_score=p1_set_score,
            opponent_set_score=p2_set_score,
            service_stats=service, return_stats=ret, point_stats=pts,
        ))

        rows.append(_normalize_player_set(
            snapshot_ts, ingest_run_id, stats_url, year, tournament_id, match_id,
            match_date, tournament_name, surface, round_name, match_duration,
            p2_id, p2_name, p1_id, p1_name,
            is_winner=(p2_id == winning_player_id),
            set_number=set_num,
            set_score=p2_set_score,
            opponent_set_score=p1_set_score,
            service_stats=p2_service, return_stats=p2_ret, point_stats=p2_pts,
        ))

    return rows


# ------------------------------------------------------------------ #
# Main ingest                                                          #
# ------------------------------------------------------------------ #

def run_ingest(start_year: int, end_year: int, sleep_seconds: float, truncate: bool) -> Dict:
    snapshot_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    ingest_run_id = str(uuid.uuid4())
    client = _bq_client()

    if truncate:
        client.query(f"TRUNCATE TABLE `{_table('website_hawkeye_match_stats')}`").result()
        print("[hawkeye] Truncated website_hawkeye_match_stats", flush=True)

    # Fetch distinct stats URLs from website_match_results
    query = f"""
        SELECT DISTINCT
            stats_url,
            MAX(match_date) as match_date
        FROM `{_dataset()}.website_match_results`
        WHERE stats_url IS NOT NULL
          AND match_date IS NOT NULL
          AND EXTRACT(YEAR FROM match_date) BETWEEN {start_year} AND {end_year}
        GROUP BY stats_url
        ORDER BY match_date DESC
    """
    urls = [(row["stats_url"], str(row["match_date"])) for row in client.query(query).result()]
    print(f"[hawkeye] Found {len(urls)} distinct stats URLs for {start_year}-{end_year}", flush=True)

    # Skip already-ingested URLs
    existing = set()
    if not truncate:
        existing_query = f"""
            SELECT DISTINCT stats_url
            FROM `{_dataset()}.website_hawkeye_match_stats`
            WHERE EXTRACT(YEAR FROM match_date) BETWEEN {start_year} AND {end_year}
        """
        try:
            existing = {row["stats_url"] for row in client.query(existing_query).result()}
            print(f"[hawkeye] Skipping {len(existing)} already-ingested URLs", flush=True)
        except Exception:
            pass

    rows_buffer: List[Dict] = []
    fetched = 0
    skipped = 0
    failed = 0
    total_rows = 0

    for i, (stats_url, match_date) in enumerate(urls, start=1):
        if stats_url in existing:
            skipped += 1
            continue

        parsed = _parse_stats_url(stats_url)
        if not parsed:
            failed += 1
            continue

        year, tid, mid = parsed
        data = _fetch_match_stats(year, tid, mid)

        if not data:
            print(f"[hawkeye] [{i}/{len(urls)}] FAILED {stats_url}", flush=True)
            failed += 1
        else:
            match_rows = _parse_response(data, stats_url, year, tid, mid, match_date, snapshot_ts, ingest_run_id)
            rows_buffer.extend(match_rows)
            fetched += 1
            if i % 50 == 0:
                print(f"[hawkeye] [{i}/{len(urls)}] fetched={fetched} failed={failed} buffered={len(rows_buffer)}", flush=True)

        # Flush buffer every 500 rows
        if len(rows_buffer) >= 500:
            total_rows += _insert_rows(client, "website_hawkeye_match_stats", rows_buffer)
            rows_buffer = []

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    # Final flush
    if rows_buffer:
        total_rows += _insert_rows(client, "website_hawkeye_match_stats", rows_buffer)

    return {
        "snapshot_ts_utc": snapshot_ts,
        "ingest_run_id": ingest_run_id,
        "start_year": start_year,
        "end_year": end_year,
        "urls_found": len(urls),
        "fetched": fetched,
        "skipped": skipped,
        "failed": failed,
        "rows_written": total_rows,
    }


def main() -> None:
    import json as _json
    parser = argparse.ArgumentParser(description="ATP Hawkeye match stats ingest")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--truncate", action=argparse.BooleanOptionalAction, default=False)
    args = parser.parse_args()

    result = run_ingest(
        start_year=args.start_year,
        end_year=args.end_year,
        sleep_seconds=args.sleep,
        truncate=args.truncate,
    )
    print(_json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
