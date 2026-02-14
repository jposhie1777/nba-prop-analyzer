# enrich_pga_player_headshots.py
# ESPN-based headshot enrichment for PGA golf players.
# Uses ESPN's bulk athletes roster endpoint to avoid unreliable search API.

import os
import re
import time
import requests
from datetime import datetime, timezone
from unicodedata import normalize

from google.cloud import bigquery
from bq import get_bq_client

# ======================================================
# CONFIG
# ======================================================

# Bulk roster: returns all PGA athletes with ESPN IDs
ESPN_PGA_ATHLETES_URL = (
    "https://sports.core.api.espn.com/v3/sports/golf/pga/athletes"
)

# Fallback: individual athlete detail (returns headshot.href)
ESPN_ATHLETE_DETAIL_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/golf/pga/athletes/{id}"
)

HEADSHOT_TEMPLATE = (
    "https://a.espncdn.com/i/headshots/golf/players/full/{id}.png"
)

REQUEST_DELAY_SEC = 0.5  # be polite to ESPN


# ======================================================
# PROJECT / CLIENT HELPERS
# ======================================================

def get_project_id() -> str:
    project = (
        os.environ.get("GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    if not project:
        raise RuntimeError("GCP_PROJECT / GOOGLE_CLOUD_PROJECT not set")
    return project


def get_bq():
    return get_bq_client()


def get_player_lookup_table() -> str:
    return f"{get_project_id()}.pga_data.player_lookup"


# ======================================================
# NAME NORMALIZATION (handles accents, casing, hyphens)
# ======================================================

def normalize_name(name: str) -> str:
    """Normalize a player name for fuzzy matching."""
    if not name:
        return ""
    # NFKD decomposition strips accents
    s = normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[^a-z ]", "", s)       # drop hyphens, apostrophes, etc.
    s = re.sub(r"\s+", " ", s)           # collapse whitespace
    return s


# ======================================================
# ESPN BULK ATHLETE FETCH
# ======================================================

def fetch_espn_pga_roster():
    """
    Fetch all PGA athletes from ESPN's bulk roster endpoint.
    Returns a dict mapping normalized_name -> { espn_id, displayName }.
    Pages through results if needed.
    """
    roster = {}
    page = 1
    limit = 500

    while True:
        print(f"  Fetching ESPN PGA athletes page {page} ...")
        resp = requests.get(
            ESPN_PGA_ATHLETES_URL,
            params={"limit": limit, "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            # items can be full objects or $ref links
            espn_id = item.get("id")
            display_name = item.get("displayName") or item.get("fullName")

            if espn_id and display_name:
                key = normalize_name(display_name)
                roster[key] = {
                    "espn_id": int(espn_id),
                    "displayName": display_name,
                }

            # If item is a $ref link, extract the ID from the URL
            ref = item.get("$ref", "")
            if not espn_id and ref:
                match = re.search(r"/athletes/(\d+)", ref)
                if match:
                    roster_id = int(match.group(1))
                    roster[f"__ref_{roster_id}"] = {
                        "espn_id": roster_id,
                        "displayName": None,
                    }

        page_count = data.get("pageCount", 1)
        if page >= page_count:
            break
        page += 1
        time.sleep(REQUEST_DELAY_SEC)

    print(f"  ESPN roster loaded: {len(roster)} entries")
    return roster


def resolve_athlete_detail(espn_id: int):
    """
    Fetch a single athlete's detail to get their displayName and headshot.
    """
    try:
        resp = requests.get(
            ESPN_ATHLETE_DETAIL_URL.format(id=espn_id),
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "espn_id": int(data.get("id", espn_id)),
            "displayName": data.get("displayName", ""),
            "headshot": data.get("headshot", {}).get("href"),
        }
    except Exception as e:
        print(f"  Could not resolve ESPN athlete {espn_id}: {e}")
        return None


def build_espn_index():
    """
    Build a normalized-name -> espn_id index from the ESPN roster.
    Resolves $ref entries that are missing display names.
    """
    roster = fetch_espn_pga_roster()

    # Resolve any $ref-only entries (missing displayName)
    refs_to_resolve = [
        v for k, v in roster.items()
        if k.startswith("__ref_") and v["displayName"] is None
    ]

    if refs_to_resolve:
        print(f"  Resolving {len(refs_to_resolve)} $ref athletes ...")
        for entry in refs_to_resolve:
            detail = resolve_athlete_detail(entry["espn_id"])
            if detail and detail["displayName"]:
                key = normalize_name(detail["displayName"])
                roster[key] = {
                    "espn_id": detail["espn_id"],
                    "displayName": detail["displayName"],
                }
            time.sleep(REQUEST_DELAY_SEC)

    # Remove __ref_ placeholders
    index = {
        k: v for k, v in roster.items()
        if not k.startswith("__ref_")
    }

    print(f"  Final ESPN index: {len(index)} named athletes")
    return index


def match_player(player_name: str, espn_index: dict):
    """
    Try to match a player name against the ESPN index.
    Tries exact normalized match first, then last-name match.
    """
    key = normalize_name(player_name)

    # Exact match
    if key in espn_index:
        return espn_index[key]

    # Last name match (for names like "J. Thomas" vs "Justin Thomas")
    parts = key.split()
    if parts:
        last = parts[-1]
        candidates = [
            v for k, v in espn_index.items()
            if k.split()[-1] == last and k.split()[0][0] == parts[0][0]
        ]
        if len(candidates) == 1:
            return candidates[0]

    return None


# ======================================================
# BIGQUERY READ (PGA PLAYERS — DEDUPED)
# ======================================================

def fetch_pga_players():
    """
    Pull the latest row per player_id from pga_data.players.
    """
    bq = get_bq()
    project = get_project_id()

    query = f"""
    SELECT
      player_id,
      ANY_VALUE(display_name) AS display_name,
      ANY_VALUE(first_name)   AS first_name,
      ANY_VALUE(last_name)    AS last_name
    FROM `{project}.pga_data.players`
    GROUP BY player_id
    """

    rows = bq.query(query).result()
    return [
        {
            "player_id": r.player_id,
            "player_name": r.display_name or f"{r.first_name or ''} {r.last_name or ''}".strip(),
        }
        for r in rows
    ]


# ======================================================
# BIGQUERY UPSERT
# ======================================================

PLAYER_LOOKUP_SCHEMA = [
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("espn_player_id", "INT64"),
    bigquery.SchemaField("espn_display_name", "STRING"),
    bigquery.SchemaField("player_image_url", "STRING"),
    bigquery.SchemaField("last_verified", "TIMESTAMP"),
    bigquery.SchemaField("source", "STRING"),
]


def ensure_player_lookup_table():
    """Create pga_data.player_lookup if it doesn't exist."""
    bq = get_bq()
    table_id = get_player_lookup_table()

    try:
        bq.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=PLAYER_LOOKUP_SCHEMA)
        bq.create_table(table)
        print(f"Created table {table_id}")


def upsert_player(player_id: int, player_name: str, espn_row: dict):
    """
    Upsert into pga_data.player_lookup using canonical player_id.
    """
    bq = get_bq()
    table_id = get_player_lookup_table()

    query = f"""
    MERGE `{table_id}` t
    USING (
      SELECT @player_id AS player_id
    ) s
    ON t.player_id = s.player_id
    WHEN MATCHED THEN
      UPDATE SET
        player_name        = @player_name,
        espn_player_id     = @espn_player_id,
        espn_display_name  = @espn_display_name,
        player_image_url   = @player_image_url,
        last_verified      = @last_verified,
        source             = @source
    WHEN NOT MATCHED THEN
      INSERT (
        player_id,
        player_name,
        espn_player_id,
        espn_display_name,
        player_image_url,
        last_verified,
        source
      )
      VALUES (
        @player_id,
        @player_name,
        @espn_player_id,
        @espn_display_name,
        @player_image_url,
        @last_verified,
        @source
      )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            bigquery.ScalarQueryParameter("player_name", "STRING", player_name),
            bigquery.ScalarQueryParameter("espn_player_id", "INT64", espn_row["espn_player_id"]),
            bigquery.ScalarQueryParameter("espn_display_name", "STRING", espn_row["espn_display_name"]),
            bigquery.ScalarQueryParameter("player_image_url", "STRING", espn_row["player_image_url"]),
            bigquery.ScalarQueryParameter("last_verified", "TIMESTAMP", espn_row["last_verified"]),
            bigquery.ScalarQueryParameter("source", "STRING", espn_row["source"]),
        ]
    )

    bq.query(query, job_config=job_config).result()


# ======================================================
# MAIN LOOP
# ======================================================

def main():
    ensure_player_lookup_table()

    # Step 1: Build ESPN name index (one bulk fetch, no per-player searches)
    print("Building ESPN PGA athlete index ...")
    espn_index = build_espn_index()

    if not espn_index:
        print("ERROR: Could not load ESPN PGA roster. Aborting.")
        return

    # Step 2: Load our PGA players from BigQuery
    players = fetch_pga_players()
    print(f"Loaded {len(players)} PGA players from BigQuery")

    seen = set()
    matched = 0
    missed = 0

    for p in players:
        player_id = p["player_id"]

        if player_id in seen:
            continue
        seen.add(player_id)

        name = p["player_name"]
        if not name:
            missed += 1
            continue
        espn_match = match_player(name, espn_index)

        if not espn_match:
            print(f"  No ESPN match for {name}")
            missed += 1
            continue

        espn_id = espn_match["espn_id"]
        espn_row = {
            "espn_player_id": espn_id,
            "espn_display_name": espn_match["displayName"],
            "player_image_url": HEADSHOT_TEMPLATE.format(id=espn_id),
            "last_verified": datetime.now(timezone.utc),
            "source": "espn_athletes_v3",
        }

        upsert_player(player_id, name, espn_row)
        print(f"  {name} -> ESPN ID {espn_id}")
        matched += 1

    print(f"Done. matched={matched}, missed={missed}, total={len(seen)}")


# ======================================================
# CLOUD RUN SAFE ENTRYPOINT
# ======================================================

def run_pga_headshot_ingest():
    """Safe entrypoint for Cloud Run background threads."""
    main()


if __name__ == "__main__":
    main()
