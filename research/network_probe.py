"""Capture network requests from a headless browser session using Playwright.

Example:
    python research/network_probe.py --url https://www.atptour.com --har out/atp.har
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from playwright.sync_api import sync_playwright
from rich.console import Console

console = Console()


def capture_network_requests(
    url: str,
    out_json: Path,
    har_path: Path | None = None,
    wait_seconds: int = 8,
    user_agent: str | None = None,
) -> list[dict[str, Any]]:
    """Open a page, observe requests, and save them to disk.

    Args:
        url: Target URL to inspect.
        out_json: Path for request log JSON output.
        har_path: Optional HAR output path.
        wait_seconds: Number of seconds to wait for requests after load.
        user_agent: Optional browser user-agent override.

    Returns:
        A list of request dictionaries.
    """

    out_json.parent.mkdir(parents=True, exist_ok=True)
    if har_path:
        har_path.parent.mkdir(parents=True, exist_ok=True)

    captured: list[dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context_kwargs: dict[str, Any] = {}
        if user_agent:
            context_kwargs["user_agent"] = user_agent
        if har_path:
            context_kwargs["record_har_path"] = str(har_path)
            context_kwargs["record_har_content"] = "embed"

        context = browser.new_context(**context_kwargs)
        page = context.new_page()

        def on_request(request: Any) -> None:
            """Capture request details as they happen."""

            entry = {
                "timestamp": datetime.now(UTC).isoformat(),
                "method": request.method,
                "url": request.url,
                "resource_type": request.resource_type,
                "headers": request.headers,
                "post_data": request.post_data,
            }
            captured.append(entry)
            console.log(f"[cyan]{entry['method']}[/cyan] {entry['url']}")

        page.on("request", on_request)

        console.log(f"Navigating to {url}")
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(wait_seconds * 1000)

        context.close()
        browser.close()

    out_json.write_text(json.dumps(captured, indent=2), encoding="utf-8")
    console.log(f"Saved {len(captured)} requests to {out_json}")

    if har_path:
        console.log(f"Saved HAR file to {har_path}")

    return captured


def main() -> None:
    """CLI entry point for network capture workflow."""

    parser = argparse.ArgumentParser(description="Capture browser network requests with Playwright.")
    parser.add_argument("--url", required=True, help="Target URL to inspect.")
    parser.add_argument(
        "--out-json",
        type=Path,
        default=Path("research/output/network_requests.json"),
        help="File path for JSON output.",
    )
    parser.add_argument("--har", type=Path, default=None, help="Optional HAR file path.")
    parser.add_argument("--wait-seconds", type=int, default=8, help="Extra wait time after page load.")
    parser.add_argument("--user-agent", default=None, help="Optional browser user-agent override.")
    args = parser.parse_args()

    capture_network_requests(
        url=args.url,
        out_json=args.out_json,
        har_path=args.har,
        wait_seconds=args.wait_seconds,
        user_agent=args.user_agent,
    )


if __name__ == "__main__":
    main()
