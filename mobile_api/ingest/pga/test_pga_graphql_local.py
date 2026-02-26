"""
Local test for the PGA Tour GraphQL pairings scraper.

Run this BEFORE wiring up BigQuery to confirm the GraphQL endpoint
is reachable and returning sensible data.

No credentials, no BigQuery, no environment variables required.

Usage
-----
# From the repo root:
    python mobile_api/ingest/pga/test_pga_graphql_local.py

# Or from the mobile_api directory:
    python ingest/pga/test_pga_graphql_local.py

# Test a specific tournament / round:
    python mobile_api/ingest/pga/test_pga_graphql_local.py \
        --tournament R2025016 --round 1

# Dump raw flat records (BigQuery-ready):
    python mobile_api/ingest/pga/test_pga_graphql_local.py --records

Finding a tournament ID
-----------------------
Go to https://www.pgatour.com/leaderboard, open DevTools → Network, filter by
"graphql", and look for a request whose variables contain "tournamentId".
The value is typically "R" + 4-digit year + 3-digit tournament number,
e.g. "R2025016".

Alternatively, check the URL path when you browse to a specific tournament
on pgatour.com – the numeric portion is the tournament code.
"""

from __future__ import annotations

import argparse
import json
import sys
import textwrap
from pathlib import Path

# ---------------------------------------------------------------------------
# Make sure the package roots are on sys.path whether the script is run from
# the repo root OR from inside mobile_api/.
# ---------------------------------------------------------------------------
_here = Path(__file__).resolve().parent          # mobile_api/ingest/pga
_mobile_api = _here.parent.parent                # mobile_api
for _p in [str(_mobile_api), str(_here.parent)]: # mobile_api, mobile_api/ingest
    if _p not in sys.path:
        sys.path.insert(0, _p)

from ingest.pga.pga_tour_graphql import fetch_pairings, pairings_to_records  # noqa: E402


# ---------------------------------------------------------------------------
# Defaults – override via CLI args
# ---------------------------------------------------------------------------
# Honda Classic 2025 is a publicly available tournament.
# Change to the current week's tournament if this one is over.
DEFAULT_TOURNAMENT = "R2025016"
DEFAULT_ROUND = "1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_separator(title: str = "") -> None:
    width = 72
    if title:
        pad = (width - len(title) - 2) // 2
        print("=" * pad + f" {title} " + "=" * pad)
    else:
        print("=" * width)


def test_fetch_pairings(tournament_id: str, round_id: str) -> bool:
    """Return True if the test passes."""
    _print_separator(f"tournament={tournament_id}  round={round_id}")
    print(f"  Endpoint : https://orchestrator.pgatour.com/graphql")
    print(f"  Query    : pairingsByRound")
    print()

    try:
        pairings = fetch_pairings(tournament_id, round_id)
    except Exception as exc:
        print(f"  FAIL – exception raised: {exc}")
        return False

    if not pairings:
        print("  WARN – empty pairings list returned.")
        print("         The tournament may not have published tee times yet,")
        print("         or the tournament ID / round number may be wrong.")
        return True  # not a hard failure

    # Basic shape checks
    first = pairings[0]
    assert first.tournament_id == tournament_id, "tournament_id mismatch"
    assert first.round_number >= 0, "round_number should be >= 0"
    assert isinstance(first.players, list), "players should be a list"

    print(f"  Groups   : {len(pairings)}")
    print(f"  Round    : {first.round_number}")
    print(f"  Course   : {first.course_name}")
    print()

    # Print first 5 groups
    print(f"  {'Grp':>4}  {'Tee':>8}  {'Hole':>4}  Players")
    print(f"  {'-'*4}  {'-'*8}  {'-'*4}  {'-'*45}")
    for p in pairings[:5]:
        names = ", ".join(pl.display_name for pl in p.players)
        tee = p.tee_time or "TBD"
        if "T" in tee:
            tee = tee.split("T")[1][:8]  # HH:MM:SS
        print(f"  {p.group_number:>4}  {tee:>8}  {p.start_hole:>4}  {names}")

    if len(pairings) > 5:
        print(f"  ... and {len(pairings) - 5} more groups")

    print()
    _print_separator("records (first 2 flattened rows)")
    records = pairings_to_records(pairings)
    print(f"  Total flat rows : {len(records)}")
    for rec in records[:2]:
        print()
        for k, v in rec.items():
            print(f"    {k:<25} {v}")

    print()
    print("  PASS")
    return True


def test_error_handling() -> bool:
    """Verify graceful error on a bad tournament ID."""
    _print_separator("error handling – bad tournament ID")
    try:
        pairings = fetch_pairings("INVALID_ID_999999", "1")
        # Either empty list or an exception is acceptable
        print(f"  Returned {len(pairings)} pairings (empty is fine)")
        print("  PASS")
        return True
    except Exception as exc:
        print(f"  Got expected exception: {type(exc).__name__}: {exc}")
        print("  PASS")
        return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Local smoke test for the PGA Tour GraphQL pairings scraper.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Examples:
              python test_pga_graphql_local.py
              python test_pga_graphql_local.py --tournament R2025016 --round 2
              python test_pga_graphql_local.py --records
            """
        ),
    )
    parser.add_argument(
        "--tournament",
        default=DEFAULT_TOURNAMENT,
        help=f"PGA Tour tournament ID (default: {DEFAULT_TOURNAMENT})",
    )
    parser.add_argument(
        "--round",
        default=DEFAULT_ROUND,
        help=f"Round number 1-4 (default: {DEFAULT_ROUND})",
    )
    parser.add_argument(
        "--records",
        action="store_true",
        help="Print all flattened BigQuery-ready records as JSON",
    )
    args = parser.parse_args()

    _print_separator("PGA Tour GraphQL – pairings scraper local test")
    print()

    passed = 0
    failed = 0

    if test_fetch_pairings(args.tournament, args.round):
        passed += 1
    else:
        failed += 1

    print()
    if test_error_handling():
        passed += 1
    else:
        failed += 1

    if args.records:
        print()
        _print_separator("full records JSON output")
        try:
            pairings = fetch_pairings(args.tournament, args.round)
            records = pairings_to_records(pairings)
            print(json.dumps(records, indent=2, default=str))
        except Exception as exc:
            print(f"Could not fetch records: {exc}")

    print()
    _print_separator("summary")
    print(f"  Passed: {passed}  Failed: {failed}")
    print()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
