"""
Injury data ingestion from BallDontLie API.

Fetches current player injuries and stores them in BigQuery.
"""

import os
import time
import requests
from datetime import datetime, timezone
from typing import Any, Optional

from bq import get_bq_client

# ==================================================
# Constants
# ==================================================
BDL_BASE_V1 = "https://api.balldontlie.io/v1"
INJURIES_TABLE = "nba_live.player_injuries"
REQUEST_DELAY_SEC = 0.3
BATCH_SIZE = 100

UTC_TZ = timezone.utc


def get_bdl_headers() -> dict[str, str]:
    """Get BallDontLie API headers with auth."""
    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise RuntimeError("BALLDONTLIE_API_KEY is missing")
    return {
        "Authorization": api_key,
        "Accept": "application/json",
    }


# ==================================================
# Fetch injuries from API
# ==================================================
def fetch_all_injuries(
    *,
    team_ids: Optional[list[int]] = None,
    player_ids: Optional[list[int]] = None,
    max_pages: int = 100,
) -> list[dict[str, Any]]:
    """
    Fetch all player injuries from BallDontLie API.

    API Endpoint: GET /v1/injuries

    Returns list of injury records with player/team info.
    """
    headers = get_bdl_headers()
    all_data: list[dict[str, Any]] = []
    cursor: Optional[int] = None
    page = 0

    print(f"[INJURIES] Fetching injuries from BallDontLie API...")

    while page < max_pages:
        page += 1

        params: dict[str, Any] = {"per_page": BATCH_SIZE}

        if team_ids:
            params["team_ids[]"] = team_ids
        if player_ids:
            params["player_ids[]"] = player_ids
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(
                f"{BDL_BASE_V1}/injuries",
                params=params,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()

        except requests.exceptions.HTTPError as e:
            print(f"[INJURIES] HTTP error fetching injuries: {e}")
            raise

        result = resp.json()
        data = result.get("data", [])
        meta = result.get("meta", {})

        if not data:
            print(f"[INJURIES] No more data at page {page}")
            break

        all_data.extend(data)
        print(f"[INJURIES] Page {page}: fetched {len(data)} injuries (total: {len(all_data)})")

        next_cursor = meta.get("next_cursor")
        if next_cursor is None:
            print(f"[INJURIES] Reached end of pagination at page {page}")
            break

        cursor = next_cursor
        time.sleep(REQUEST_DELAY_SEC)

    print(f"[INJURIES] Total injuries fetched: {len(all_data)}")
    return all_data


# ==================================================
# Transform API response to BigQuery row
# ==================================================
def transform_injury_to_row(injury: dict[str, Any], run_ts: str) -> dict[str, Any]:
    """
    Transform a single injury API record to BigQuery row.

    Expected API response shape:
    {
        "id": 123,
        "player": {
            "id": 456,
            "first_name": "Anthony",
            "last_name": "Davis",
            ...
        },
        "team": {
            "id": 14,
            "abbreviation": "LAL",
            "full_name": "Los Angeles Lakers",
            ...
        },
        "status": "Out",
        "comment": "Ankle",
        "date": "2025-01-15",
        "return_date": null
    }
    """
    player = injury.get("player") or {}
    team = injury.get("team") or {}

    # Parse report date
    report_date = None
    if injury.get("date"):
        try:
            report_date = injury["date"]
        except (ValueError, AttributeError):
            pass

    # Parse return date
    return_date = None
    if injury.get("return_date"):
        try:
            return_date = injury["return_date"]
        except (ValueError, AttributeError):
            pass

    return {
        # Metadata
        "injury_id": injury.get("id"),
        "run_ts": run_ts,
        "ingested_at": datetime.now(UTC_TZ).isoformat(),

        # Player info
        "player_id": player.get("id"),
        "player_first_name": player.get("first_name"),
        "player_last_name": player.get("last_name"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),

        # Team info
        "team_id": team.get("id"),
        "team_abbreviation": team.get("abbreviation"),
        "team_name": team.get("full_name"),

        # Injury details
        "status": injury.get("status"),  # "Out", "Questionable", "Doubtful", "Day-To-Day", "Probable"
        "injury_type": injury.get("comment"),  # "Ankle", "Knee", "Rest", etc.
        "report_date": report_date,
        "return_date": return_date,
    }


# ==================================================
# BigQuery operations
# ==================================================
def insert_rows_to_bq(rows: list[dict[str, Any]], table: str) -> int:
    """Insert rows into BigQuery table."""
    if not rows:
        return 0

    client = get_bq_client()

    # Insert in batches of 500
    batch_size = 500
    total_inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = client.insert_rows_json(table, batch)

        if errors:
            print(f"[INJURIES] BigQuery insert errors: {errors[:5]}")
            raise RuntimeError(f"BigQuery insert failed: {errors[:3]}")

        total_inserted += len(batch)

    print(f"[INJURIES] Successfully inserted {total_inserted} rows into {table}")
    return total_inserted


def clear_old_injuries(table: str = INJURIES_TABLE) -> int:
    """
    Clear old injury records before inserting fresh data.
    This ensures we always have the latest injury report.
    """
    client = get_bq_client()

    # Delete all existing records (we'll insert fresh snapshot)
    query = f"DELETE FROM `{table}` WHERE TRUE"

    try:
        job = client.query(query)
        job.result()  # Wait for completion
        print(f"[INJURIES] Cleared old records from {table}")
        return job.num_dml_affected_rows or 0
    except Exception as e:
        print(f"[INJURIES] Warning: Could not clear old records: {e}")
        return 0


# ==================================================
# Public ingest functions
# ==================================================
def ingest_injuries(
    *,
    team_ids: Optional[list[int]] = None,
    player_ids: Optional[list[int]] = None,
    table: str = INJURIES_TABLE,
    clear_existing: bool = True,
) -> dict[str, Any]:
    """
    Ingest current player injuries into BigQuery.

    Args:
        team_ids: Optional list of team IDs to filter
        player_ids: Optional list of player IDs to filter
        table: BigQuery table to insert into
        clear_existing: Whether to clear old records first (default True)

    Returns:
        Dict with ingest results
    """
    print(f"\n[INJURIES] ========== INGEST START ==========")
    print(f"[INJURIES] Time: {datetime.now(UTC_TZ).isoformat()}")

    run_ts = datetime.now(UTC_TZ).isoformat()

    try:
        # 1. Clear old records if requested
        deleted = 0
        if clear_existing:
            deleted = clear_old_injuries(table)

        # 2. Fetch current injuries
        injuries = fetch_all_injuries(
            team_ids=team_ids,
            player_ids=player_ids,
        )

        if not injuries:
            print(f"[INJURIES] No injuries found")
            return {
                "status": "ok",
                "injuries_fetched": 0,
                "injuries_inserted": 0,
                "deleted": deleted,
                "run_ts": run_ts,
            }

        # 3. Transform to rows
        rows = [transform_injury_to_row(inj, run_ts) for inj in injuries]

        # 4. Insert to BigQuery
        inserted = insert_rows_to_bq(rows, table)

        # 5. Summary by status
        status_counts = {}
        for row in rows:
            status = row.get("status", "Unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        print(f"[INJURIES] ========== INGEST COMPLETE ==========")
        print(f"[INJURIES] Injuries by status: {status_counts}")

        return {
            "status": "ok",
            "injuries_fetched": len(injuries),
            "injuries_inserted": inserted,
            "deleted": deleted,
            "status_breakdown": status_counts,
            "run_ts": run_ts,
        }

    except Exception as e:
        print(f"[INJURIES] ERROR: {e}")
        return {
            "status": "error",
            "error": str(e),
            "run_ts": run_ts,
        }


def get_current_injuries(
    *,
    team_id: Optional[int] = None,
    status: Optional[str] = None,
    table: str = INJURIES_TABLE,
) -> list[dict[str, Any]]:
    """
    Get current injuries from BigQuery.

    Args:
        team_id: Optional team ID to filter
        status: Optional status filter ("Out", "Questionable", etc.)

    Returns:
        List of injury records
    """
    client = get_bq_client()

    where_clauses = ["TRUE"]

    if team_id:
        where_clauses.append(f"team_id = {team_id}")

    if status:
        safe_status = status.replace("'", "''")
        where_clauses.append(f"status = '{safe_status}'")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        injury_id,
        player_id,
        player_name,
        player_first_name,
        player_last_name,
        team_id,
        team_abbreviation,
        team_name,
        status,
        injury_type,
        report_date,
        return_date,
        ingested_at
    FROM `{table}`
    WHERE {where_sql}
    ORDER BY team_abbreviation, status, player_last_name
    """

    rows = [dict(r) for r in client.query(query).result()]
    return rows
