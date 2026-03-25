"""
ATP historical results capture script.

Uses Camoufox to fetch each tournament's archive results page and saves
the HTML to the local filesystem. website_ingest.py then reads these
files instead of fetching live.

Output structure:
    website_responses/atp/historical/{year}/{slug}_{tid}

Usage:
    python -m mobile_api.ingest.atp.capture_atp_historical \
        --start-year 2019 --end-year 2025 --sleep 2.0

    # Dry run (list tournaments without fetching)
    python -m mobile_api.ingest.atp.capture_atp_historical \
        --start-year 2022 --end-year 2022 --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple


# ------------------------------------------------------------------ #
# Constants                                                            #
# ------------------------------------------------------------------ #

ATP_BASE = "https://www.atptour.com"
ARCHIVE_PAGE = ATP_BASE + "/en/scores/results-archive?year={year}"
RESULTS_PAGE = ATP_BASE + "/en/scores/archive/{slug}/{tid}/{year}/results"
DEFAULT_OUTPUT_ROOT = Path("website_responses/atp/historical")

_BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ------------------------------------------------------------------ #
# Tournament discovery                                                 #
# ------------------------------------------------------------------ #

def _discover_tournaments(year: int, page: object) -> List[Tuple[str, str]]:
    """
    Navigate to the ATP results-archive page for a given year and extract
    all (slug, tournament_id) pairs from archive result links.
    Returns a deduplicated list preserving page order.
    """
    url = ARCHIVE_PAGE.format(year=year)
    print(f"[capture] Discovering tournaments for {year}: {url}")

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)
    except Exception as exc:
        print(f"[capture] WARNING: failed to load archive page year={year}: {exc}")
        return []

    try:
        html: str = page.content()
    except Exception as exc:
        print(f"[capture] WARNING: failed to get page content year={year}: {exc}")
        return []

    pattern = re.compile(
        r'href="/en/scores/archive/([^/]+)/([^/]+)/' + str(year) + r'/results"',
        re.IGNORECASE,
    )
    seen = set()
    results = []
    for m in pattern.finditer(html):
        slug, tid = m.group(1), m.group(2)
        key = (slug, tid)
        if key not in seen:
            seen.add(key)
            results.append(key)

    print(f"[capture] Found {len(results)} tournaments for {year}")
    return results


# ------------------------------------------------------------------ #
# Single tournament fetch                                              #
# ------------------------------------------------------------------ #

def _fetch_results_page(
    slug: str,
    tid: str,
    year: int,
    page: object,
    timeout_ms: int = 30000,
) -> Optional[str]:
    """
    Navigate to a tournament's archive results page and return the HTML.
    Returns None on failure.
    """
    url = RESULTS_PAGE.format(slug=slug, tid=tid, year=year)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(2000)

        # Wait for match content to appear — results pages render match
        # divs server-side so they should be present immediately, but give
        # JS a moment to finish any hydration.
        try:
            page.wait_for_selector('div.match', timeout=10000)
        except Exception:
            pass  # Some tournaments may have no matches (e.g. Davis Cup country results)

        html: str = page.content()
        if not html or len(html) < 500:
            print(f"[capture] WARNING: suspiciously short response for {slug}/{tid}/{year}")
            return None
        return html
    except Exception as exc:
        print(f"[capture] ERROR fetching {slug}/{tid}/{year}: {exc}")
        return None


# ------------------------------------------------------------------ #
# File I/O                                                             #
# ------------------------------------------------------------------ #

def _output_path(output_root: Path, year: int, slug: str, tid: str) -> Path:
    return output_root / str(year) / f"{slug}_{tid}"


def _write_capture_file(path: Path, url: str, html: str) -> None:
    """Write HTML in the same capture-file format used by website_ingest.py."""
    path.parent.mkdir(parents=True, exist_ok=True)
    content = f"<!--\n{url}\n-->\n{html}"
    path.write_text(content, encoding="utf-8")


def _already_captured(path: Path) -> bool:
    """Return True if a non-empty capture file already exists at this path."""
    return path.exists() and path.stat().st_size > 500


# ------------------------------------------------------------------ #
# Main capture loop                                                    #
# ------------------------------------------------------------------ #

def capture(
    start_year: int,
    end_year: int,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    sleep_seconds: float = 2.0,
    dry_run: bool = False,
    resume: bool = True,
) -> dict:
    """
    Fetch and save ATP archive results pages for all tournaments across
    the requested year range.

    Args:
        start_year:    First year to capture (inclusive).
        end_year:      Last year to capture (inclusive).
        output_root:   Root directory for saved HTML files.
        sleep_seconds: Delay between requests (be respectful to ATP servers).
        dry_run:       If True, discover tournaments but don't fetch HTML.
        resume:        If True, skip tournaments already captured on disk.

    Returns:
        Summary dict with counts.
    """
    from camoufox.sync_api import Camoufox

    total_found = 0
    total_fetched = 0
    total_skipped = 0
    total_failed = 0
    per_year: dict = {}

    with Camoufox(headless=True, geoip=True) as browser:
        context = browser.new_context(
            locale="en-US",
            viewport={"width": 1920, "height": 1080},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = context.new_page()

        # Prime session cookies via ATP homepage, same pattern as OddspediaClient.
        print("[capture] Priming session via ATP homepage...")
        try:
            page.goto(ATP_BASE, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000)
        except Exception as exc:
            print(f"[capture] WARNING: homepage prime failed: {exc}")

        for year in range(start_year, end_year + 1):
            year_found = 0
            year_fetched = 0
            year_skipped = 0
            year_failed = 0

            tournaments = _discover_tournaments(year, page)
            year_found = len(tournaments)
            total_found += year_found

            if dry_run:
                for slug, tid in tournaments:
                    path = _output_path(output_root, year, slug, tid)
                    status = "EXISTS" if _already_captured(path) else "MISSING"
                    print(f"  [{status}] {year}/{slug}_{tid}")
                per_year[year] = {"found": year_found, "dry_run": True}
                continue

            for i, (slug, tid) in enumerate(tournaments, start=1):
                path = _output_path(output_root, year, slug, tid)

                if resume and _already_captured(path):
                    print(f"[capture] [{i}/{year_found}] SKIP (exists) {year}/{slug}_{tid}")
                    year_skipped += 1
                    total_skipped += 1
                    continue

                print(f"[capture] [{i}/{year_found}] Fetching {year}/{slug}_{tid}...")
                html = _fetch_results_page(slug, tid, year, page)

                if html:
                    url = RESULTS_PAGE.format(slug=slug, tid=tid, year=year)
                    _write_capture_file(path, url, html)
                    print(f"[capture]   Saved {path} ({len(html):,} bytes)")
                    year_fetched += 1
                    total_fetched += 1
                else:
                    year_failed += 1
                    total_failed += 1

                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

            per_year[year] = {
                "found": year_found,
                "fetched": year_fetched,
                "skipped": year_skipped,
                "failed": year_failed,
            }
            print(
                f"[capture] Year {year} complete: "
                f"found={year_found} fetched={year_fetched} "
                f"skipped={year_skipped} failed={year_failed}"
            )

        browser.close()

    summary = {
        "start_year": start_year,
        "end_year": end_year,
        "output_root": str(output_root),
        "dry_run": dry_run,
        "total_found": total_found,
        "total_fetched": total_fetched,
        "total_skipped": total_skipped,
        "total_failed": total_failed,
        "per_year": per_year,
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }
    return summary


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture ATP historical results pages using Camoufox"
    )
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=f"Root directory for saved files (default: {DEFAULT_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="Seconds to sleep between requests (default: 2.0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover tournaments and show what would be fetched, without fetching",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Re-fetch even if a capture file already exists",
    )
    args = parser.parse_args()

    result = capture(
        start_year=args.start_year,
        end_year=args.end_year,
        output_root=args.output_root,
        sleep_seconds=args.sleep,
        dry_run=args.dry_run,
        resume=not args.no_resume,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
