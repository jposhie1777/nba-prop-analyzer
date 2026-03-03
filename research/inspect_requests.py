"""Utilities for replaying cURL commands and stored HTTP request payloads.

This module is intentionally small and composable so it can be imported into
notebooks/tests or executed from the command line.
"""

from __future__ import annotations

import argparse
import json
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests
from rich.console import Console
from rich.pretty import Pretty

console = Console()


@dataclass
class RequestDefinition:
    """Represents the minimum details needed to replay an HTTP request."""

    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    data: str | None = None


def parse_curl_command(curl_command: str) -> RequestDefinition:
    """Parse a cURL command into a :class:`RequestDefinition`.

    Supports common flags used in browser-exported commands:
    - `-X/--request` for method
    - `-H/--header` for headers
    - `--data/--data-raw/-d` for body data

    Args:
        curl_command: The full cURL command string.

    Returns:
        A parsed request definition.
    """

    tokens = shlex.split(curl_command)
    if not tokens or tokens[0] != "curl":
        raise ValueError("Input must be a valid cURL command beginning with 'curl'.")

    method = "GET"
    url = ""
    headers: dict[str, str] = {}
    data: str | None = None

    i = 1
    while i < len(tokens):
        token = tokens[i]

        if token in {"-X", "--request"} and i + 1 < len(tokens):
            method = tokens[i + 1].upper()
            i += 2
            continue

        if token in {"-H", "--header"} and i + 1 < len(tokens):
            raw_header = tokens[i + 1]
            if ":" in raw_header:
                key, value = raw_header.split(":", 1)
                headers[key.strip()] = value.strip()
            i += 2
            continue

        if token in {"--data", "--data-raw", "-d"} and i + 1 < len(tokens):
            data = tokens[i + 1]
            if method == "GET":
                method = "POST"
            i += 2
            continue

        if token.startswith("http://") or token.startswith("https://"):
            url = token

        i += 1

    if not url:
        raise ValueError("Could not determine URL from cURL command.")

    return RequestDefinition(method=method, url=url, headers=headers, data=data)


def replay_request(request_def: RequestDefinition, timeout: int = 20) -> dict[str, Any]:
    """Execute a request and return a compact response report."""

    response = requests.request(
        method=request_def.method,
        url=request_def.url,
        headers=request_def.headers,
        data=request_def.data,
        timeout=timeout,
    )

    content_type = response.headers.get("content-type", "")
    parsed_body: Any
    schema: Any

    if "application/json" in content_type:
        parsed_body = response.json()
        schema = infer_json_schema(parsed_body)
    else:
        parsed_body = response.text[:800]
        schema = "non-json response"

    return {
        "request": request_def.__dict__,
        "status_code": response.status_code,
        "content_type": content_type,
        "schema": schema,
        "body_preview": parsed_body,
    }


def infer_json_schema(data: Any) -> Any:
    """Infer a lightweight JSON schema-like structure from Python objects."""

    if isinstance(data, dict):
        return {k: infer_json_schema(v) for k, v in data.items()}
    if isinstance(data, list):
        if not data:
            return []
        return [infer_json_schema(data[0])]
    return type(data).__name__


def load_request_json(path: Path) -> RequestDefinition:
    """Load a request definition from JSON file."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    return RequestDefinition(
        method=payload.get("method", "GET"),
        url=payload["url"],
        headers=payload.get("headers", {}),
        data=payload.get("data"),
    )


def main() -> None:
    """CLI entry point to replay cURL strings or saved request JSON files."""

    parser = argparse.ArgumentParser(description="Replay a cURL command or saved request JSON.")
    parser.add_argument("--curl", help="Raw cURL command to replay.")
    parser.add_argument("--request-file", type=Path, help="Path to request JSON file.")
    args = parser.parse_args()

    if bool(args.curl) == bool(args.request_file):
        parser.error("Provide exactly one of --curl or --request-file.")

    request_def = parse_curl_command(args.curl) if args.curl else load_request_json(args.request_file)
    report = replay_request(request_def)

    console.print("[bold green]Replay complete[/bold green]")
    console.print(Pretty(report))


if __name__ == "__main__":
    main()
