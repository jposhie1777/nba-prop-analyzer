from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

from atp_client import ATPClient, load_cache_hints, save_cache_hints
from atp_normalize import (
    normalize_calendar,
    normalize_head_to_head,
    normalize_match_results_html,
    normalize_match_schedule_html,
    normalize_overview,
    normalize_top_seeds,
    utc_now_iso,
)

LOGGER = logging.getLogger("atp.cli")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ATP Tour data fetcher")
    parser.add_argument("--out", default="./data", help="Output directory")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--sleep", type=float, default=0.4)
    parser.add_argument("--dry-run", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)

    calendar_parser = subparsers.add_parser("calendar", help="Fetch ATP calendar")
    calendar_parser.add_argument("--enrich-overview", action="store_true")
    calendar_parser.add_argument("--include-raw-overview-json", action="store_true")

    top_seeds_parser = subparsers.add_parser("topseeds", help="Fetch ATP top seeds for event")
    top_seeds_parser.add_argument("--tournament-id", required=True)
    top_seeds_parser.add_argument("--event-year", required=True, type=int)

    h2h_parser = subparsers.add_parser("h2h", help="Fetch ATP head-to-head history")
    h2h_parser.add_argument("--left-player-id", required=True)
    h2h_parser.add_argument("--right-player-id", required=True)

    schedule_parser = subparsers.add_parser("match-schedule", help="Fetch and parse ATP match schedule HTML")
    schedule_parser.add_argument("--tournament-slug", required=True)
    schedule_parser.add_argument("--tournament-id", required=True)
    schedule_parser.add_argument("--day", type=int)

    results_parser = subparsers.add_parser("match-results", help="Fetch and parse ATP match results HTML")
    results_parser.add_argument("--tournament-slug", required=True)
    results_parser.add_argument("--tournament-id", required=True)

    return parser.parse_args()


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = sorted({k for row in rows for k in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def save_raw_json(raw_dir: Path, name: str, payload: Dict[str, Any], snapshot_ts_utc: str) -> None:
    safe_ts = snapshot_ts_utc.replace(":", "").replace("+00:00", "Z")
    out_path = raw_dir / f"{name}_{safe_ts}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def save_raw_text(raw_dir: Path, name: str, payload: str, snapshot_ts_utc: str) -> None:
    safe_ts = snapshot_ts_utc.replace(":", "").replace("+00:00", "Z")
    out_path = raw_dir / f"{name}_{safe_ts}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(payload)


def update_cache(cache_hints: Dict[str, Dict[str, Any]], key: str, response: Dict[str, Any], payload_key: str) -> None:
    cache_hints[key] = {
        "etag": response.get("etag") or "",
        "last_modified": response.get("last_modified") or "",
        "cached_payload": response.get(payload_key),
    }


def run_calendar(args: argparse.Namespace, client: ATPClient, out_dir: Path, cache_hints: Dict[str, Dict[str, Any]]) -> None:
    raw_dir = out_dir / "raw"
    snapshot_ts_utc = utc_now_iso()

    calendar_cache_key = "/en/-/tournaments/calendar/tour"
    calendar_response = client.fetch_calendar(cache_hints=cache_hints.get(calendar_cache_key))
    calendar_json = calendar_response["fetched_json"]
    save_raw_json(raw_dir, "calendar_tour", calendar_json, snapshot_ts_utc)
    update_cache(cache_hints, calendar_cache_key, calendar_response, payload_key="fetched_json")

    month_rows, tournament_rows = normalize_calendar(calendar_json, snapshot_ts_utc=snapshot_ts_utc)
    month_dict_rows = [row.to_dict() for row in month_rows]
    tournament_dict_rows = [row.to_dict() for row in tournament_rows]

    overview_dict_rows: List[Dict[str, Any]] = []
    if args.enrich_overview:
        for tournament in tournament_rows:
            tournament_id = tournament.tournament_id
            profile_key = f"/en/-/tournaments/profile/{tournament_id}/overview"
            response = client.fetch_tournament_overview(tournament_id, cache_hints=cache_hints.get(profile_key))
            overview_json = response["fetched_json"]
            save_raw_json(raw_dir, f"overview_{tournament_id}", overview_json, snapshot_ts_utc)
            update_cache(cache_hints, profile_key, response, payload_key="fetched_json")
            overview_dict_rows.append(
                normalize_overview(
                    tournament_id=tournament_id,
                    overview_json=overview_json,
                    snapshot_ts_utc=snapshot_ts_utc,
                    include_raw_json=args.include_raw_overview_json,
                ).to_dict()
            )
            if args.sleep > 0:
                time.sleep(args.sleep)

    if args.dry_run:
        LOGGER.info("Dry run calendar rows: months=%s tournaments=%s overviews=%s", len(month_dict_rows), len(tournament_dict_rows), len(overview_dict_rows))
        return

    write_jsonl(out_dir / "tournament_months.jsonl", month_dict_rows)
    write_jsonl(out_dir / "tournaments.jsonl", tournament_dict_rows)
    write_csv(out_dir / "tournament_months.csv", month_dict_rows)
    write_csv(out_dir / "tournaments.csv", tournament_dict_rows)
    if args.enrich_overview:
        write_jsonl(out_dir / "tournaments_overview.jsonl", overview_dict_rows)
        write_csv(out_dir / "tournaments_overview.csv", overview_dict_rows)


def run_top_seeds(args: argparse.Namespace, client: ATPClient, out_dir: Path, cache_hints: Dict[str, Dict[str, Any]]) -> None:
    raw_dir = out_dir / "raw"
    snapshot_ts_utc = utc_now_iso()
    cache_key = f"/en/-/tournaments/{args.tournament_id}/{args.event_year}/topseeds"
    response = client.fetch_tournament_top_seeds(args.tournament_id, args.event_year, cache_hints=cache_hints.get(cache_key))
    payload = response["fetched_json"]
    save_raw_json(raw_dir, f"topseeds_{args.tournament_id}_{args.event_year}", payload, snapshot_ts_utc)
    update_cache(cache_hints, cache_key, response, payload_key="fetched_json")

    rows = [row.to_dict() for row in normalize_top_seeds(args.tournament_id, args.event_year, payload, snapshot_ts_utc=snapshot_ts_utc)]
    if args.dry_run:
        LOGGER.info("Dry run top seeds rows=%s", len(rows))
        return
    write_jsonl(out_dir / f"top_seeds_{args.tournament_id}_{args.event_year}.jsonl", rows)
    write_csv(out_dir / f"top_seeds_{args.tournament_id}_{args.event_year}.csv", rows)


def run_h2h(args: argparse.Namespace, client: ATPClient, out_dir: Path, cache_hints: Dict[str, Dict[str, Any]]) -> None:
    raw_dir = out_dir / "raw"
    snapshot_ts_utc = utc_now_iso()
    cache_key = f"/en/-/tour/Head2HeadSearch/GetHead2HeadData/{args.left_player_id}/{args.right_player_id}"
    response = client.fetch_head_to_head(args.left_player_id, args.right_player_id, cache_hints=cache_hints.get(cache_key))
    payload = response["fetched_json"]
    save_raw_json(raw_dir, f"h2h_{args.left_player_id}_{args.right_player_id}", payload, snapshot_ts_utc)
    update_cache(cache_hints, cache_key, response, payload_key="fetched_json")

    rows = [row.to_dict() for row in normalize_head_to_head(args.left_player_id, args.right_player_id, payload, snapshot_ts_utc=snapshot_ts_utc)]
    if args.dry_run:
        LOGGER.info("Dry run h2h rows=%s", len(rows))
        return
    write_jsonl(out_dir / f"h2h_matches_{args.left_player_id}_{args.right_player_id}.jsonl", rows)
    write_csv(out_dir / f"h2h_matches_{args.left_player_id}_{args.right_player_id}.csv", rows)


def run_match_schedule(args: argparse.Namespace, client: ATPClient, out_dir: Path, cache_hints: Dict[str, Dict[str, Any]]) -> None:
    raw_dir = out_dir / "raw"
    snapshot_ts_utc = utc_now_iso()
    key = f"/en/scores/current/{args.tournament_slug}/{args.tournament_id}/daily-schedule?day={args.day}" if args.day is not None else f"/en/scores/current/{args.tournament_slug}/{args.tournament_id}/daily-schedule"
    response = client.fetch_match_schedule_html(args.tournament_slug, args.tournament_id, day=args.day, cache_hints=cache_hints.get(key))
    html_text = response["fetched_text"]
    save_raw_text(raw_dir, f"match_schedule_{args.tournament_slug}_{args.tournament_id}", html_text, snapshot_ts_utc)
    update_cache(cache_hints, key, response, payload_key="fetched_text")

    rows = [r.to_dict() for r in normalize_match_schedule_html(args.tournament_slug, args.tournament_id, html_text, snapshot_ts_utc=snapshot_ts_utc)]
    if args.dry_run:
        LOGGER.info("Dry run match schedule rows=%s", len(rows))
        return
    suffix = f"_day{args.day}" if args.day is not None else ""
    write_jsonl(out_dir / f"match_schedule_{args.tournament_slug}_{args.tournament_id}{suffix}.jsonl", rows)
    write_csv(out_dir / f"match_schedule_{args.tournament_slug}_{args.tournament_id}{suffix}.csv", rows)


def run_match_results(args: argparse.Namespace, client: ATPClient, out_dir: Path, cache_hints: Dict[str, Dict[str, Any]]) -> None:
    raw_dir = out_dir / "raw"
    snapshot_ts_utc = utc_now_iso()
    key = f"/en/scores/current/{args.tournament_slug}/{args.tournament_id}/results"
    response = client.fetch_match_results_html(args.tournament_slug, args.tournament_id, cache_hints=cache_hints.get(key))
    html_text = response["fetched_text"]
    save_raw_text(raw_dir, f"match_results_{args.tournament_slug}_{args.tournament_id}", html_text, snapshot_ts_utc)
    update_cache(cache_hints, key, response, payload_key="fetched_text")

    rows = [r.to_dict() for r in normalize_match_results_html(args.tournament_slug, args.tournament_id, html_text, snapshot_ts_utc=snapshot_ts_utc)]
    if args.dry_run:
        LOGGER.info("Dry run match results rows=%s", len(rows))
        return
    write_jsonl(out_dir / f"match_results_{args.tournament_slug}_{args.tournament_id}.jsonl", rows)
    write_csv(out_dir / f"match_results_{args.tournament_slug}_{args.tournament_id}.csv", rows)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = parse_args()
    out_dir = Path(args.out)
    cache_file = out_dir / "raw" / "http_cache_hints.json"
    cache_hints = load_cache_hints(cache_file)
    client = ATPClient(timeout=args.timeout)

    if args.command == "calendar":
        run_calendar(args, client, out_dir, cache_hints)
    elif args.command == "topseeds":
        run_top_seeds(args, client, out_dir, cache_hints)
    elif args.command == "h2h":
        run_h2h(args, client, out_dir, cache_hints)
    elif args.command == "match-schedule":
        run_match_schedule(args, client, out_dir, cache_hints)
    elif args.command == "match-results":
        run_match_results(args, client, out_dir, cache_hints)

    save_cache_hints(cache_file, cache_hints)
    LOGGER.info("Done. Output root=%s", out_dir)


if __name__ == "__main__":
    main()
