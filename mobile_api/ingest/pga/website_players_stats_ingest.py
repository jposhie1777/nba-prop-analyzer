from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from typing import Any, Dict, List, Optional, Set

import requests
from google.cloud import bigquery

DATASET = os.getenv("PGA_DATASET", "pga_data")
LOCATION = os.getenv("PGA_DATASET_LOCATION", "US")
ACTIVE_PLAYERS_TABLE = os.getenv("PGA_WEBSITE_ACTIVE_PLAYERS_TABLE", "website_active_players")
PLAYER_STATS_TABLE = os.getenv("PGA_WEBSITE_PLAYER_STATS_TABLE", "website_player_stats")

GRAPHQL_ENDPOINT = "https://orchestrator.pgatour.com/graphql"
API_KEY = os.getenv("PGA_TOUR_GQL_API_KEY", "da2-gsrx5bibzbb4njvhl7t37wqyl4")

ACTIVE_PLAYERS_QUERY = """
query ActivePlayers($tourCode: TourCode!) {
  playerDirectory(tourCode: $tourCode) {
    players {
      id
      playerId
      firstName
      lastName
      displayName
      shortName
      amateur
      active
      country
      countryFlag
    }
  }
}
""".strip()

STAT_OVERVIEW_QUERY = """
query StatOverview($tourCode: TourCode!, $year: Int!) {
  statOverview(tourCode: $tourCode, year: $year) {
    tourCode
    year
    stats {
      statId
      statName
      tourAvg
      players {
        statId
        playerId
        statTitle
        statValue
        playerName
        rank
        country
        countryFlag
      }
    }
  }
}
""".strip()


def _headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "x-api-key": API_KEY,
        "x-pgat-platform": "web",
        "Referer": "https://www.pgatour.com/",
        "Origin": "https://www.pgatour.com",
    }


def _post_graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = requests.post(
        GRAPHQL_ENDPOINT,
        headers=_headers(),
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(f"GraphQL errors: {json.dumps(payload['errors'])}")
    return payload.get("data") or {}


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project)


def _table_id(client: bigquery.Client, table: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{client.project}.{table}"
    return f"{client.project}.{DATASET}.{table}"


def _ensure_dataset(client: bigquery.Client) -> None:
    dataset_id = f"{client.project}.{DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)


def _ensure_tables(client: bigquery.Client) -> tuple[str, str]:
    active_table_id = _table_id(client, ACTIVE_PLAYERS_TABLE)
    active_schema = [
        bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("display_name", "STRING"),
        bigquery.SchemaField("active", "BOOL"),
        bigquery.SchemaField("player_payload", "STRING"),
    ]
    active_table = bigquery.Table(active_table_id, schema=active_schema)
    active_table.clustering_fields = ["player_id"]
    active_table.description = "PGA website active players with full payload JSON"
    client.create_table(active_table, exists_ok=True)

    stats_table_id = _table_id(client, PLAYER_STATS_TABLE)
    stats_schema = [
        bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("tour_code", "STRING"),
        bigquery.SchemaField("year", "INT64"),
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("player_name", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("country_flag", "STRING"),
        bigquery.SchemaField("stats_payload", "STRING"),
        bigquery.SchemaField("tour_averages", "STRING"),
    ]
    stats_table = bigquery.Table(stats_table_id, schema=stats_schema)
    stats_table.range_partitioning = bigquery.RangePartitioning(
        field="year",
        range_=bigquery.PartitionRange(start=2015, end=2035, interval=1),
    )
    stats_table.clustering_fields = ["tour_code", "year", "player_id"]
    stats_table.description = "PGA website player stats (one row per player per year)"
    client.create_table(stats_table, exists_ok=True)

    return active_table_id, stats_table_id


def _flatten_player_directory(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = data.get("playerDirectory")
    if root is None:
        return []
    if isinstance(root, list):
        players = root
    elif isinstance(root, dict):
        players = root.get("players") or root.get("items") or root.get("rows") or []
    else:
        players = []

    rows: List[Dict[str, Any]] = []
    for player in players:
        player_id = str(player.get("id") or player.get("playerId") or "").strip()
        if not player_id:
            continue
        active_val = player.get("active")
        is_active = True if active_val is None else bool(active_val)
        rows.append(
            {
                "player_id": player_id,
                "display_name": player.get("displayName") or player.get("playerName") or player.get("name"),
                "active": is_active,
                "player_payload": json.dumps(player, separators=(",", ":"), default=str),
            }
        )
    return rows


def _group_player_stats(data: Dict[str, Any], current_year: int, active_player_ids: Set[str]) -> List[Dict[str, Any]]:
    overview = (data or {}).get("statOverview") or {}
    year = int(overview.get("year") or 0)
    if year != current_year:
        return []

    grouped: Dict[str, Dict[str, Any]] = {}
    for stat in overview.get("stats") or []:
        stat_id = str(stat.get("statId") or "")
        stat_name = stat.get("statName")
        tour_avg = stat.get("tourAvg")
        for row in stat.get("players") or []:
            player_id = str(row.get("playerId") or "").strip()
            if not player_id or (active_player_ids and player_id not in active_player_ids):
                continue
            item = grouped.setdefault(
                player_id,
                {
                    "player_id": player_id,
                    "player_name": row.get("playerName"),
                    "country": row.get("country"),
                    "country_flag": row.get("countryFlag"),
                    "stats": {},
                    "tour_averages": {},
                },
            )
            item["stats"][stat_id] = {
                "stat_id": stat_id,
                "stat_name": stat_name,
                "stat_title": row.get("statTitle"),
                "stat_value": row.get("statValue"),
                "rank": row.get("rank"),
                "tour_avg": tour_avg,
            }
            item["tour_averages"][stat_id] = tour_avg

    return list(grouped.values())


def ingest_website_players_and_stats(
    year: Optional[int] = None,
    tour_code: str = "R",
    *,
    refresh_active_players: bool = True,
) -> Dict[str, int]:
    year = year or dt.datetime.utcnow().year
    now = dt.datetime.utcnow().isoformat()

    active_rows: List[Dict[str, Any]] = []
    active_player_ids: Set[str] = set()
    if refresh_active_players:
        players_raw = _post_graphql(ACTIVE_PLAYERS_QUERY, {"tourCode": tour_code})
        active_rows = _flatten_player_directory(players_raw)
        active_player_ids = {r["player_id"] for r in active_rows if r.get("active")}

    stats_rows: List[Dict[str, Any]] = []

    client = _bq_client()
    _ensure_dataset(client)
    active_table_id, stats_table_id = _ensure_tables(client)

    client.query(f"TRUNCATE TABLE `{active_table_id}`").result()
    client.query(
        f"DELETE FROM `{stats_table_id}` WHERE year = @year AND tour_code = @tour_code",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("year", "INT64", int(year)),
                bigquery.ScalarQueryParameter("tour_code", "STRING", tour_code),
            ]
        ),
    ).result()

    active_payload = []
    if refresh_active_players:
        active_payload = [
            {
                "run_ts": now,
                "ingested_at": now,
                **row,
            }
            for row in active_rows
            if row.get("active")
        ]
    else:
        # Reuse active player IDs from latest table snapshot when skipping refresh.
        latest_ids_sql = f"SELECT DISTINCT player_id FROM `{active_table_id}`"
        for row in client.query(latest_ids_sql).result():
            pid = str(row.get("player_id") or "").strip()
            if pid:
                active_player_ids.add(pid)

    stats_raw = _post_graphql(STAT_OVERVIEW_QUERY, {"tourCode": tour_code, "year": year})
    stats_rows = _group_player_stats(stats_raw, year, active_player_ids)

    stats_payload = [
        {
            "run_ts": now,
            "ingested_at": now,
            "tour_code": tour_code,
            "year": year,
            "player_id": row["player_id"],
            "player_name": row.get("player_name"),
            "country": row.get("country"),
            "country_flag": row.get("country_flag"),
            "stats_payload": json.dumps(row.get("stats") or {}, separators=(",", ":"), default=str),
            "tour_averages": json.dumps(row.get("tour_averages") or {}, separators=(",", ":"), default=str),
        }
        for row in stats_rows
    ]

    for start in range(0, len(active_payload), 500):
        errors = client.insert_rows_json(active_table_id, active_payload[start : start + 500])
        if errors:
            raise RuntimeError(f"active player insert errors: {errors[:2]}")
        time.sleep(0.05)

    for start in range(0, len(stats_payload), 500):
        errors = client.insert_rows_json(stats_table_id, stats_payload[start : start + 500])
        if errors:
            raise RuntimeError(f"player stats insert errors: {errors[:2]}")
        time.sleep(0.05)

    return {
        "active_players_fetched": len(active_rows),
        "active_players_written": len(active_payload),
        "stats_players_written": len(stats_payload),
    }


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Ingest PGA website active players + player stats")
    parser.add_argument("--year", type=int, default=dt.datetime.utcnow().year)
    parser.add_argument("--tour", default="R")
    args = parser.parse_args()
    print(ingest_website_players_and_stats(year=args.year, tour_code=args.tour))


if __name__ == "__main__":
    _cli()
