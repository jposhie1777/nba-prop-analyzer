"""
Introspect the PGA Tour GraphQL schema to discover real query/field names.

Run this to find the correct operation names before updating the scraper:
    python mobile_api/ingest/pga/introspect_pga_schema.py

Modes
-----
  (default)   Tee-time query return types + Player type drill-down
  --all       Print every top-level query field with args
  --discover  Search for stat / leaderboard / scorecard / schedule / rank /
              course / player / shot queries and drill into their return types

Copy the output and share it so the scraper query can be fixed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_here = Path(__file__).resolve().parent
_mobile_api = _here.parent.parent
for _p in [str(_mobile_api), str(_here.parent)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import requests

ENDPOINT = "https://orchestrator.pgatour.com/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": "da2-gsrx5bibzbb4njvhl7t37wqyl4",
    "x-pgat-platform": "web",
    "Referer": "https://www.pgatour.com/",
    "Origin": "https://www.pgatour.com",
}


def introspect_queries() -> None:
    """Print all top-level Query fields with their argument names."""
    query = """
    {
      __schema {
        queryType {
          fields {
            name
            description
            args {
              name
              type { name kind ofType { name kind ofType { name kind } } }
            }
          }
        }
      }
    }
    """
    resp = requests.post(ENDPOINT, headers=HEADERS, json={"query": query}, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        print("Schema introspection errors:")
        print(json.dumps(data["errors"], indent=2))
        return

    fields = data["data"]["__schema"]["queryType"]["fields"]
    print(f"\nPGA Tour GraphQL — {len(fields)} top-level Query fields:\n")
    for f in sorted(fields, key=lambda x: x["name"]):
        args = f.get("args", [])

        def type_str(t: dict) -> str:
            if t.get("name"):
                return t["name"]
            inner = t.get("ofType") or {}
            if inner.get("name"):
                return f"{t['kind']}({inner['name']})"
            inner2 = inner.get("ofType") or {}
            return f"{t['kind']}({inner.get('kind', '')}({inner2.get('name', '?')}))"

        arg_str = ", ".join(f"{a['name']}: {type_str(a['type'])}" for a in args)
        print(f"  {f['name']}({arg_str})")
        if f.get("description"):
            print(f"    ↳ {f['description']}")


def search_pairing_fields() -> None:
    """Look for any field whose name contains 'pair', 'tee', 'field', or 'group'."""
    query = """
    {
      __schema {
        queryType {
          fields { name description }
        }
      }
    }
    """
    resp = requests.post(ENDPOINT, headers=HEADERS, json={"query": query}, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        return

    keywords = {"pair", "tee", "group", "field", "round", "lineup", "start"}
    fields = data["data"]["__schema"]["queryType"]["fields"]
    matches = [
        f for f in fields
        if any(kw in f["name"].lower() for kw in keywords)
    ]

    print(f"\nFields matching pairing/tee-time keywords:\n")
    if matches:
        for f in matches:
            print(f"  *** {f['name']}")
            if f.get("description"):
                print(f"      {f['description']}")
    else:
        print("  (none found — print full list above to find correct name)")


def introspect_type(type_name: str) -> None:
    """Recursively print fields on a named type."""
    query = """
    query IntrospectType($name: String!) {
      __type(name: $name) {
        name
        kind
        fields {
          name
          description
          type { name kind ofType { name kind ofType { name kind ofType { name kind } } } }
        }
      }
    }
    """
    resp = requests.post(
        ENDPOINT, headers=HEADERS,
        json={"query": query, "variables": {"name": type_name}},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    t = (data.get("data") or {}).get("__type")
    if not t:
        print(f"  (type '{type_name}' not found)")
        return
    fields = t.get("fields") or []
    print(f"\n  type {type_name} ({len(fields)} fields):")
    for f in fields:
        def rtype(tp: dict, depth: int = 0) -> str:
            if tp is None:
                return "?"
            if tp.get("name"):
                return tp["name"]
            inner = tp.get("ofType") or {}
            return f"{tp['kind']}({rtype(inner, depth+1)})"
        print(f"    {f['name']}: {rtype(f['type'])}")
        if f.get("description"):
            print(f"      ↳ {f['description']}")


def introspect_query_return_types() -> None:
    """Show return types for teeTimes, teeTimesV2, and tournamentGroupLocations."""
    # First get the return type names for each query
    query = """
    {
      __schema {
        queryType {
          fields {
            name
            type { name kind ofType { name kind ofType { name kind } } }
          }
        }
      }
    }
    """
    resp = requests.post(ENDPOINT, headers=HEADERS, json={"query": query}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    fields = data["data"]["__schema"]["queryType"]["fields"]

    targets = {"teeTimes", "teeTimesV2", "teeTimesCompressedV2", "tournamentGroupLocations", "groupLocations"}
    print("\n--- Return type introspection for tee-time queries ---")
    for f in fields:
        if f["name"] not in targets:
            continue

        def rtype(tp: dict) -> str:
            if tp is None:
                return "?"
            if tp.get("name"):
                return tp["name"]
            return rtype(tp.get("ofType") or {})

        ret = rtype(f["type"])
        print(f"\n  {f['name']} → {ret}")
        introspect_type(ret)


def _fetch_all_query_fields() -> list:
    """Return the full list of top-level Query fields with args and return types."""
    query = """
    {
      __schema {
        queryType {
          fields {
            name
            description
            args {
              name
              defaultValue
              type { name kind ofType { name kind ofType { name kind } } }
            }
            type { name kind ofType { name kind ofType { name kind } } }
          }
        }
      }
    }
    """
    resp = requests.post(ENDPOINT, headers=HEADERS, json={"query": query}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        print("Introspection errors:", json.dumps(data["errors"], indent=2))
        return []
    return data["data"]["__schema"]["queryType"]["fields"]


def _resolve_type_name(tp: dict) -> str:
    if not tp:
        return "?"
    if tp.get("name"):
        return tp["name"]
    inner = tp.get("ofType")
    if inner is None:
        return "?"
    return _resolve_type_name(inner)


def _type_sig(tp: dict) -> str:
    """Human-readable type string including NON_NULL / LIST wrappers."""
    if tp is None:
        return "?"
    kind = tp.get("kind", "")
    name = tp.get("name")
    inner = tp.get("ofType")
    if name:
        return name
    if kind == "NON_NULL":
        return f"{_type_sig(inner)}!"
    if kind == "LIST":
        return f"[{_type_sig(inner)}]"
    return _type_sig(inner) if inner else "?"


def discover_data_fields() -> None:
    """
    Search for stat / leaderboard / scorecard / schedule / rank / course /
    player / shot query fields, print their args, then drill into return types.
    """
    KEYWORD_GROUPS = {
        "stats":       {"stat", "strokes", "gained", "driving", "putting", "approach"},
        "leaderboard": {"leaderboard", "standings", "scoreboard", "results"},
        "scorecards":  {"scorecard", "holescore", "hole", "score"},
        "schedule":    {"schedule", "calendar", "tournament"},
        "rankings":    {"rank", "ranking", "fedex", "owgr", "points"},
        "course":      {"course", "hole", "yardage", "layout"},
        "player":      {"player", "profile", "bio", "athlete"},
        "shot":        {"shot", "shotlink", "trackman", "strokes"},
    }

    all_fields = _fetch_all_query_fields()
    if not all_fields:
        return

    # Build a name→field lookup
    by_name = {f["name"]: f for f in all_fields}

    # Group matches (a field can appear in multiple groups)
    groups: dict[str, list] = {g: [] for g in KEYWORD_GROUPS}
    for fname, field in by_name.items():
        lower = fname.lower()
        desc_lower = (field.get("description") or "").lower()
        for group_name, keywords in KEYWORD_GROUPS.items():
            if any(kw in lower or kw in desc_lower for kw in keywords):
                groups[group_name].append(field)

    # Track which return types we've already drilled into
    drilled: set[str] = set()

    for group_name, matches in groups.items():
        if not matches:
            continue
        print(f"\n{'='*60}")
        print(f"  CATEGORY: {group_name.upper()}  ({len(matches)} queries)")
        print(f"{'='*60}")

        for f in sorted(matches, key=lambda x: x["name"]):
            args = f.get("args", [])
            arg_parts = []
            for a in args:
                default = f" = {a['defaultValue']}" if a.get("defaultValue") is not None else ""
                arg_parts.append(f"{a['name']}: {_type_sig(a['type'])}{default}")
            arg_str = ", ".join(arg_parts) if arg_parts else ""
            ret_type = _resolve_type_name(f["type"])
            print(f"\n  {f['name']}({arg_str}) → {_type_sig(f['type'])}")
            if f.get("description"):
                print(f"    ↳ {f['description']}")

            # Drill into the return type once
            if ret_type and ret_type not in drilled and ret_type not in {
                "String", "Int", "Float", "Boolean", "ID", "?",
            }:
                drilled.add(ret_type)
                introspect_type(ret_type)

    print(f"\n{'='*60}")
    print(f"  Drilled into {len(drilled)} return type(s): {sorted(drilled)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PGA Tour GraphQL schema introspection")
    parser.add_argument(
        "--all", action="store_true",
        help="Print every top-level query field with args",
    )
    parser.add_argument(
        "--discover", action="store_true",
        help="Search stat/leaderboard/scorecard/schedule/rank/course/player/shot queries",
    )
    args = parser.parse_args()

    print("Connecting to", ENDPOINT)

    if args.all:
        introspect_queries()
    elif args.discover:
        discover_data_fields()
    else:
        # Default: tee-time return types + Player drill-down (original behaviour)
        introspect_query_return_types()
        print("\n--- Nested type drill-down ---")
        for t in ["Player"]:
            introspect_type(t)
