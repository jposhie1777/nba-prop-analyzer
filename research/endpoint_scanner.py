"""Simple endpoint scanner to evaluate known API paths with shared headers.

This script sends bounded, low-volume requests and stores structured results to
support reverse engineering workflows.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import httpx
from rich.console import Console
from rich.table import Table

console = Console()


def load_headers(config_path: Path) -> dict[str, str]:
    """Load request headers from a JSON config file."""

    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return payload.get("common_headers", {})


def load_endpoints(path: Path) -> list[str]:
    """Load endpoint URLs from text file (one URL per line)."""

    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def scan_endpoints(
    endpoints: list[str],
    headers: dict[str, str],
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    """Send GET requests to candidate endpoints and collect response metadata."""

    results: list[dict[str, Any]] = []
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        for endpoint in endpoints:
            try:
                response = client.get(endpoint, headers=headers)
                result = {
                    "url": endpoint,
                    "status_code": response.status_code,
                    "content_type": response.headers.get("content-type", ""),
                    "content_length": len(response.content),
                    "is_json": "application/json" in response.headers.get("content-type", ""),
                }
            except Exception as exc:  # noqa: BLE001 - scanner should continue on network errors
                result = {
                    "url": endpoint,
                    "error": str(exc),
                }
            results.append(result)
    return results


def render_results_table(results: list[dict[str, Any]]) -> None:
    """Print scanner output in a compact rich table."""

    table = Table(title="Endpoint Scan Results")
    table.add_column("URL", overflow="fold")
    table.add_column("Status")
    table.add_column("Type")
    table.add_column("Length")

    for row in results:
        table.add_row(
            row.get("url", ""),
            str(row.get("status_code", row.get("error", "error"))),
            row.get("content_type", "-"),
            str(row.get("content_length", "-")),
        )

    console.print(table)


def main() -> None:
    """CLI entry point for endpoint scanner."""

    parser = argparse.ArgumentParser(description="Scan known API endpoints and summarize responses.")
    parser.add_argument(
        "--endpoints-file",
        type=Path,
        default=Path("research/config/endpoints.txt"),
        help="Text file with one URL per line.",
    )
    parser.add_argument(
        "--headers-config",
        type=Path,
        default=Path("research/config/headers.json"),
        help="Path to header configuration JSON.",
    )
    parser.add_argument(
        "--out-json",
        type=Path,
        default=Path("research/output/endpoint_scan.json"),
        help="Result JSON path.",
    )
    args = parser.parse_args()

    endpoints = load_endpoints(args.endpoints_file)
    headers = load_headers(args.headers_config)
    results = scan_endpoints(endpoints=endpoints, headers=headers)

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(results, indent=2), encoding="utf-8")

    render_results_table(results)
    console.print(f"Saved scan report to {args.out_json}")


if __name__ == "__main__":
    main()
