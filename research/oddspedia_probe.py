"""Capture Oddspedia API responses using a headless browser session.

Navigates the Oddspedia tennis odds page with Playwright, intercepts every
API call made by the page, and saves the response bodies to
``website_responses/oddspedia/`` so the shapes can be studied offline.

In addition to the default page load, the script:
  1. Clicks each "1st Set" / "2nd Set" / "Total" tab header it finds on the
     page so those on-demand API calls are triggered and captured.
  2. Clicks the first listed match to open the right-panel detail view,
     triggering per-match market endpoints.

All captured responses are written as:
    <out_dir>/<endpoint_slug>[_<ot_or_key>]

where the file contains the raw HTTP headers followed by the response body,
matching the format of the existing files in website_responses/oddspedia/.

Usage (from repo root):
    python research/oddspedia_probe.py

    # Custom URL or output directory:
    python research/oddspedia_probe.py \\
        --url https://www.oddspedia.com/us/tennis \\
        --out website_responses/oddspedia \\
        --wait 6
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import Response, sync_playwright
from rich.console import Console

console = Console()

# API base path we care about
_API_PREFIX = "oddspedia.com/api/v1/"

# Tab labels to click (matches text inside the tab buttons on the page)
_TABS_TO_CLICK = ["1st Set", "2nd Set", "Total"]


def _slug_for_url(url: str) -> str:
    """Derive a filesystem-safe filename stem from an API URL.

    Examples:
        getAmericanMaxOddsWithPagination?...&ot=204  →  american_odds_ot204
        getMatchList?...                             →  match_list
        getBookmakers?...                            →  bookmakers
    """
    parsed = urlparse(url)
    endpoint = parsed.path.rstrip("/").split("/")[-1]

    # CamelCase → snake_case
    slug = re.sub(r"(?<=[a-z])([A-Z])", r"_\1", endpoint).lower()
    slug = re.sub(r"^get_", "", slug)  # drop leading "get_"

    qs = parse_qs(parsed.query)
    if "ot" in qs:
        slug = f"{slug}_ot{qs['ot'][0]}"
    elif "sport" in qs:
        sport = qs["sport"][0]
        slug = f"{slug}_{sport}"

    return slug


def _format_response_file(url: str, status: int, headers: dict, body: str) -> str:
    """Format a response as HEADERS block + body, matching existing file style."""
    lines = [
        "HEADERS",
        "Request URL",
        url,
        "Request Method",
        "GET",
        f"Status Code",
        f"{status} {'OK' if status == 200 else 'ERROR'}",
        "",
    ]
    for k, v in headers.items():
        lines.append(k.lower())
        lines.append(v)
    lines.append("")
    lines.append(body)
    return "\n".join(lines)


def probe_oddspedia(
    url: str,
    out_dir: Path,
    wait_seconds: int = 6,
    user_agent: str = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
) -> list[dict[str, Any]]:
    """Load the Oddspedia tennis page, interact with tabs, and capture API responses.

    Saves each captured API response to ``out_dir/<slug>`` and returns a
    summary list of what was captured.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    captured: list[dict[str, Any]] = []

    try:
        from playwright_stealth import stealth_sync as _stealth
    except ImportError:
        _stealth = None

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=user_agent,
            locale="en-US",
            viewport={"width": 1920, "height": 1080},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = context.new_page()
        if _stealth:
            _stealth(page)

        # ── Response handler ──────────────────────────────────────────────────
        def on_response(response: Response) -> None:
            if _API_PREFIX not in response.url:
                return
            try:
                body = response.text()
            except Exception:
                body = ""

            slug = _slug_for_url(response.url)
            out_path = out_dir / slug
            out_path.write_text(
                _format_response_file(
                    url=response.url,
                    status=response.status,
                    headers=dict(response.headers),
                    body=body,
                ),
                encoding="utf-8",
            )

            entry = {
                "timestamp": datetime.now(UTC).isoformat(),
                "url": response.url,
                "status": response.status,
                "slug": slug,
                "saved_to": str(out_path),
            }
            captured.append(entry)
            console.log(f"[green]{response.status}[/green] {slug}  [dim]{response.url[:90]}[/dim]")

        page.on("response", on_response)

        # ── Initial page load ────────────────────────────────────────────────
        console.log(f"[bold]Navigating to:[/bold] {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_function("() => !!window.__NUXT__", timeout=15_000)
        page.wait_for_timeout(wait_seconds * 1000)

        # ── Click tab buttons to trigger per-set API calls ───────────────────
        for tab_label in _TABS_TO_CLICK:
            try:
                # Tab buttons use text content matching the label
                tab = page.locator(
                    f"button:has-text('{tab_label}'), "
                    f"[role='tab']:has-text('{tab_label}'), "
                    f".tab:has-text('{tab_label}')"
                ).first
                if tab.is_visible(timeout=3_000):
                    console.log(f"Clicking tab: [yellow]{tab_label}[/yellow]")
                    tab.click()
                    page.wait_for_timeout(2_000)
                else:
                    console.log(f"[dim]Tab not visible: {tab_label}[/dim]")
            except Exception as exc:
                console.log(f"[dim]Could not click '{tab_label}': {exc}[/dim]")

        # ── Click the first match row to open the right-panel detail view ────
        # This triggers per-match endpoints (correct score, set-by-set markets)
        try:
            first_match = page.locator(".match-url, .match-row").first
            if first_match.is_visible(timeout=3_000):
                console.log("Clicking first match row to open detail panel …")
                first_match.click()
                page.wait_for_timeout(3_000)
            else:
                console.log("[dim]No match rows found to click[/dim]")
        except Exception as exc:
            console.log(f"[dim]Could not click match row: {exc}[/dim]")

        # ── Extra wait for any lazy-loaded requests ───────────────────────────
        page.wait_for_timeout(2_000)

        browser.close()

    # ── Write summary ─────────────────────────────────────────────────────────
    summary_path = out_dir / "_probe_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "probed_url": url,
                "run_at": datetime.now(UTC).isoformat(),
                "captured_count": len(captured),
                "files": captured,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    console.log(
        f"\n[bold green]Done.[/bold green] "
        f"Captured {len(captured)} API responses → {out_dir}"
    )
    return captured


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture Oddspedia tennis API responses via headless browser."
    )
    parser.add_argument(
        "--url",
        default="https://www.oddspedia.com/us/tennis",
        help="Oddspedia page URL to probe (default: tennis scores/odds page)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("website_responses/oddspedia"),
        help="Directory to save captured responses (default: website_responses/oddspedia)",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=6,
        help="Seconds to wait after page load before interacting (default: 6)",
    )
    args = parser.parse_args()

    probe_oddspedia(url=args.url, out_dir=args.out, wait_seconds=args.wait)


if __name__ == "__main__":
    main()
