"""
Introspect the PGA Tour GraphQL schema to discover real query/field names.

Run this to find the correct operation names before updating the scraper:
    python mobile_api/ingest/pga/introspect_pga_schema.py

Copy the output and share it so the scraper query can be fixed.
"""

from __future__ import annotations

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


if __name__ == "__main__":
    print("Connecting to", ENDPOINT)
    introspect_query_return_types()

    # Drill into the nested types that contain actual pairing/player data
    print("\n--- Nested type drill-down ---")
    for t in [
        "Player",
    ]:
        introspect_type(t)
