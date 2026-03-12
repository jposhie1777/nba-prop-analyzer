"""
Introspect the Oddspedia REST API to discover available endpoints,
their parameters, and the field schemas of their responses.

Run this to understand what data is available before extending the scraper:
    python mobile_api/ingest/atp/introspect_oddspedia_schema.py

Modes
-----
  (default)   Show key endpoint schemas (matchList fields, odds, bookmakers,
              leagues) using the bundled cached responses — no network needed
  --all       Print every known endpoint with its params and full response schema
  --discover  Categorise all response fields by data type
              (match / odds / bookmaker / league / category)
  --file PATH Parse a saved Oddspedia HTML page and show the __NUXT__ state
              structure (equivalent to what oddspedia_client.py extracts)
  --live      Same as default but fetches from the live Oddspedia API instead
              of using cached files (requires internet access)

Copy the output and share it so the scraper can be extended with new fields.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Path setup ────────────────────────────────────────────────────────────────
_here       = Path(__file__).resolve().parent          # mobile_api/ingest/atp/
_repo_root  = _here.parents[2]                         # repo root
_cached_dir = _repo_root / "website_responses" / "oddspedia"

for _p in [str(_repo_root)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import requests  # noqa: E402

# ── API constants ─────────────────────────────────────────────────────────────

BASE_URL = "https://oddspedia.com/api/v1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer":         "https://www.oddspedia.com/",
    "Origin":          "https://www.oddspedia.com",
    "Accept":          "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

# Known endpoints: name → (path, default_params, cached_file_name)
KNOWN_ENDPOINTS: Dict[str, Tuple[str, Dict[str, str], Optional[str]]] = {
    "getMatchList": (
        "/getMatchList",
        {
            "sport":               "tennis",
            "geoCode":             "US",
            "language":            "us",
            "sortBy":              "default",
            "excludeSpecialStatus": "0",
        },
        "tennis_match_list",
    ),
    "getMatchCount": (
        "/getMatchCount",
        {
            "r":        "wv",
            "inplay":   "1",
            "language": "us",
        },
        "tennis_match_count",
    ),
    "getBookmakers": (
        "/getBookmakers",
        {"geoCode": "", "geoState": "", "language": "us"},
        "tennis_bookmakers",
    ),
    "getLeagues": (
        "/getLeagues",
        {
            "sport":                        "tennis",
            "geoCode":                      "US",
            "language":                     "us",
            "topLeaguesOnly":               "1",
            "includeLeaguesWithoutMatches": "1",
        },
        "tennis_leagues",
    ),
    "getCategories": (
        "/getCategories",
        {
            "sport":        "tennis",
            "geoCode":      "US",
            "language":     "us",
            "countriesOnly": "0",
        },
        "tennis_categories",
    ),
    "getAmericanMaxOddsWithPagination": (
        "/getAmericanMaxOddsWithPagination",
        {
            "geoCode":   "US",
            "sport":     "tennis",
            "language":  "us",
        },
        "tennis_get_american_odds",
    ),
}

# ── Node.js snippet (mirrors oddspedia_client.py) ─────────────────────────────

_NODE_EXTRACTOR = r"""
const fs   = require('fs');
const vm   = require('vm');
const html = fs.readFileSync(process.argv[2], 'utf8');

const m = html.match(/window\.__NUXT__\s*=\s*([\s\S]*?)<\/script>/);
if (!m) { process.stderr.write('__NUXT__ block not found\n'); process.exit(1); }

const nuxt = vm.runInContext(m[1].trim().replace(/;$/, ''), vm.createContext({}));
process.stdout.write(JSON.stringify(nuxt));
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _type_label(value: Any) -> str:
    """Return a human-readable type label for a JSON value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return f"str  (e.g. {repr(value)[:40]})"
    if isinstance(value, list):
        if not value:
            return "list[]"
        item_type = _type_label(value[0]).split()[0]
        return f"list[{len(value)}×{item_type}]"
    if isinstance(value, dict):
        return f"dict({len(value)} keys)"
    return type(value).__name__


def _print_schema(obj: Any, indent: int = 0, max_depth: int = 3) -> None:
    """Recursively print the schema of *obj* with indentation."""
    pad = "  " * indent
    if isinstance(obj, dict):
        for key, val in obj.items():
            label = _type_label(val)
            print(f"{pad}{key}: {label}")
            if indent < max_depth and isinstance(val, (dict, list)):
                _print_schema(val, indent + 1, max_depth)
    elif isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            print(f"{pad}[item schema — first of {len(obj)}]")
            _print_schema(obj[0], indent + 1, max_depth)
    else:
        print(f"{pad}{_type_label(obj)}")


def _load_cached_json(filename: str) -> Optional[Dict[str, Any]]:
    """Load JSON from a cached response file (header block + JSON body)."""
    path = _cached_dir / filename
    if not path.exists():
        return None

    content = path.read_text(encoding="utf-8", errors="replace")
    lines = content.split("\n")
    for i, line in enumerate(lines):
        stripped = line.strip()
        if (stripped.startswith("{") or stripped.startswith("[")) and "%22" not in stripped[:20]:
            try:
                return json.loads("\n".join(lines[i:]))
            except json.JSONDecodeError:
                # May be multiple JSON objects; take just the first
                try:
                    decoder = json.JSONDecoder()
                    data, _ = decoder.raw_decode("\n".join(lines[i:]))
                    return data
                except Exception:
                    return None
    return None


def _fetch_live(path: str, params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Fetch a live Oddspedia API endpoint and return parsed JSON."""
    url = BASE_URL + path
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"  ERROR fetching {url}: {exc}")
        return None


def _extract_nuxt_from_html_file(html_path: Path) -> Optional[Dict[str, Any]]:
    """Run the Node.js extractor against an HTML file and return the __NUXT__ dict."""
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", encoding="utf-8", delete=False) as js_tmp:
        js_tmp.write(_NODE_EXTRACTOR)
        js_path = js_tmp.name

    try:
        result = subprocess.run(
            ["node", js_path, str(html_path)],
            capture_output=True,
            text=True,
            timeout=30,
        )
    finally:
        Path(js_path).unlink(missing_ok=True)

    if result.returncode != 0:
        print(f"  Node.js extractor failed: {result.stderr.strip()}")
        return None

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"  JSON parse error: {exc}")
        return None


# ── Introspection functions ────────────────────────────────────────────────────

def show_endpoint_schema(
    name: str,
    path: str,
    params: Dict[str, str],
    cached_file: Optional[str],
    *,
    live: bool = False,
    max_depth: int = 3,
) -> None:
    """Print the response schema for a single endpoint."""
    print(f"\n{'='*60}")
    print(f"  ENDPOINT: {name}")
    print(f"  URL:      {BASE_URL}{path}")
    print(f"  PARAMS:   {json.dumps(params, separators=(',', ':'))}")
    print(f"{'='*60}")

    data: Optional[Dict[str, Any]] = None

    if live:
        print(f"  [fetching live …]")
        data = _fetch_live(path, params)
    elif cached_file:
        data = _load_cached_json(cached_file)
        if data is None:
            print(f"  [cached file '{cached_file}' not found — use --live to fetch]")
            return
        print(f"  [loaded from cached file: {cached_file}]")
    else:
        print(f"  [no cached file — use --live to fetch]")
        return

    if data is None:
        return

    print()
    _print_schema(data, indent=1, max_depth=max_depth)


def introspect_all_endpoints(*, live: bool = False) -> None:
    """Print full schema for every known endpoint."""
    print(f"\nOddspedia REST API — {len(KNOWN_ENDPOINTS)} known endpoints\n")
    for name, (path, params, cached) in KNOWN_ENDPOINTS.items():
        show_endpoint_schema(name, path, params, cached, live=live, max_depth=4)


def introspect_key_endpoints(*, live: bool = False) -> None:
    """Default mode: show the most useful endpoints (matchList and odds)."""
    priority = ["getMatchList", "getAmericanMaxOddsWithPagination", "getBookmakers", "getLeagues"]
    print(f"\nOddspedia REST API — key endpoint schemas\n")
    for name in priority:
        if name in KNOWN_ENDPOINTS:
            path, params, cached = KNOWN_ENDPOINTS[name]
            show_endpoint_schema(name, path, params, cached, live=live)


def discover_data_fields(*, live: bool = False) -> None:
    """
    Categorise all response fields into semantic groups:
    match / odds / bookmaker / league / category.
    """
    CATEGORY_RULES: Dict[str, List[str]] = {
        "match":      ["match", "game", "score", "period", "inplay", "status",
                       "winner", "home", "away", "team", "player"],
        "odds":       ["odd", "price", "handicap", "moneyline", "spread",
                       "market", "decimal", "american"],
        "bookmaker":  ["book", "bookie", "slug", "rating", "order"],
        "league":     ["league", "tournament", "competition", "round"],
        "location":   ["category", "country", "geo", "region", "sport"],
    }

    # Gather all field names across all endpoints
    all_field_paths: Dict[str, List[str]] = {}  # field_name → [endpoint, ...]

    def _walk(obj: Any, prefix: str, endpoint_name: str) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                full = f"{prefix}.{k}" if prefix else k
                all_field_paths.setdefault(k, [])
                if endpoint_name not in all_field_paths[k]:
                    all_field_paths[k].append(endpoint_name)
                _walk(v, full, endpoint_name)
        elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
            _walk(obj[0], prefix + "[*]", endpoint_name)

    print("\nFetching all endpoint data …")
    for name, (path, params, cached) in KNOWN_ENDPOINTS.items():
        if live:
            data = _fetch_live(path, params)
        else:
            data = _load_cached_json(cached) if cached else None
        if data:
            _walk(data, "", name)

    # Categorise
    categorised: Dict[str, List[Tuple[str, List[str]]]] = {c: [] for c in CATEGORY_RULES}
    uncategorised: List[Tuple[str, List[str]]] = []

    for field, sources in sorted(all_field_paths.items()):
        lower = field.lower()
        matched = False
        for cat, keywords in CATEGORY_RULES.items():
            if any(kw in lower for kw in keywords):
                categorised[cat].append((field, sources))
                matched = True
                break
        if not matched:
            uncategorised.append((field, sources))

    for cat, items in categorised.items():
        if not items:
            continue
        print(f"\n{'='*60}")
        print(f"  CATEGORY: {cat.upper()}  ({len(items)} fields)")
        print(f"{'='*60}")
        for field, sources in items:
            src_str = ", ".join(sources)
            print(f"  {field:<35} ← {src_str}")

    if uncategorised:
        print(f"\n{'='*60}")
        print(f"  UNCATEGORISED  ({len(uncategorised)} fields)")
        print(f"{'='*60}")
        for field, sources in uncategorised:
            src_str = ", ".join(sources)
            print(f"  {field:<35} ← {src_str}")

    total = sum(len(v) for v in categorised.values()) + len(uncategorised)
    print(f"\n  Total unique field names discovered: {total}")


def introspect_nuxt_file(html_path: Path) -> None:
    """Parse a saved Oddspedia HTML file and show the __NUXT__ state structure."""
    print(f"\n{'='*60}")
    print(f"  __NUXT__ state introspection")
    print(f"  File: {html_path}")
    print(f"{'='*60}")

    nuxt = _extract_nuxt_from_html_file(html_path)
    if nuxt is None:
        print("  Failed to extract __NUXT__ state.")
        return

    print(f"\n  Top-level __NUXT__ keys: {list(nuxt.keys())}")

    # data[0] is the page-specific payload (mirrors oddspedia_client.py)
    data_arr = nuxt.get("data") or []
    if not data_arr:
        print("\n  (no 'data' array found in __NUXT__)")
        _print_schema(nuxt, indent=1)
        return

    data0 = data_arr[0] if isinstance(data_arr, list) else data_arr
    print(f"\n  data[0] keys: {list(data0.keys()) if isinstance(data0, dict) else type(data0).__name__}")
    print()

    if isinstance(data0, dict):
        for key, val in data0.items():
            label = _type_label(val)
            print(f"  {key}: {label}")
            if isinstance(val, dict) and val:
                for k2, v2 in list(val.items())[:5]:
                    print(f"    {k2}: {_type_label(v2)}")
                if len(val) > 5:
                    print(f"    … (+{len(val)-5} more keys)")
            elif isinstance(val, list) and val and isinstance(val[0], dict):
                print(f"    [item schema — first of {len(val)}]")
                for k2, v2 in list(val[0].items())[:8]:
                    print(f"      {k2}: {_type_label(v2)}")
                if len(val[0]) > 8:
                    print(f"      … (+{len(val[0])-8} more keys)")

    print(f"\n  Sections also present in __NUXT__ (outside data[0]):")
    for key in nuxt:
        if key != "data":
            print(f"    {key}: {_type_label(nuxt[key])}")


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Oddspedia REST API schema introspection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Print every known endpoint with params and full response schema",
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Categorise all response fields (match / odds / bookmaker / league / category)",
    )
    parser.add_argument(
        "--file",
        metavar="PATH",
        help="Parse a saved Oddspedia HTML page and show the __NUXT__ state structure",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Fetch from the live Oddspedia API instead of cached files",
    )
    args = parser.parse_args()

    if args.file:
        introspect_nuxt_file(Path(args.file))
    elif args.all:
        introspect_all_endpoints(live=args.live)
    elif args.discover:
        discover_data_fields(live=args.live)
    else:
        # Default: key endpoint schemas
        introspect_key_endpoints(live=args.live)
