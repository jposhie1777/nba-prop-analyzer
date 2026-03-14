from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from google.cloud import bigquery

# Market files expected under --input-dir (website_responses/atp/ by default).
# Keys are filenames; values are the slug written into the BQ row.
MARKET_FILE_TO_SLUG = {
    "moneyline": "moneyline",       # marketGroupId=201 – Match Winner
    "set_handicap": "set_handicap", # marketGroupId=301 – +/-1.5 Sets / Game Handicap
}

MARKET_TABLES = {
    "moneyline": "atp_odds_moneyline",
    "set_handicap": "atp_odds_set_handicap",
}


def _dataset() -> str:
    return os.getenv("ODDSPEDIA_DATASET", "oddspedia")


def _location() -> str:
    return os.getenv("ODDSPEDIA_BQ_LOCATION", "US")


def _project() -> str | None:
    return os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")


def _get_client() -> bigquery.Client:
    project = _project()
    return bigquery.Client(project=project) if project else bigquery.Client()


def _extract_json_payload(text: str) -> dict[str, Any]:
    marker = re.search(r"\nResponse\n|\nRESPONSE\n", text)
    if marker:
        start = text.find("{", marker.end())
    else:
        start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in capture")
    return json.loads(text[start:])


def _normalize_market_rows(payload: dict[str, Any], market_slug: str) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    periods = {str(p.get("id")): p.get("name") for p in payload.get("periods", [])}
    outcome_names = data.get("outcome_names", [])

    generated_at_raw = payload.get("generated_at")
    generated_at = None
    if isinstance(generated_at_raw, str):
        generated_at = generated_at_raw.replace(" ", "T") + "Z"

    rows: list[dict[str, Any]] = []
    for period_id, period_obj in data.get("odds", {}).items():
        entries: list[tuple[str | None, dict[str, Any]]] = []
        if isinstance(period_obj, dict) and "odds" in period_obj:
            entries.append((None, period_obj))
        elif isinstance(period_obj, dict) and "main" in period_obj:
            main_obj = period_obj.get("main")
            if isinstance(main_obj, dict):
                entries.append(("main", main_obj))
            for alt in period_obj.get("alternative", []):
                if isinstance(alt, dict):
                    entries.append(("alternative", alt))

        for line_type, entry in entries:
            line_name = entry.get("name")
            for outcome_key, odd_obj in (entry.get("odds") or {}).items():
                idx = int(outcome_key.removeprefix("o")) - 1
                outcome_name = outcome_names[idx] if 0 <= idx < len(outcome_names) else outcome_key

                rows.append(
                    {
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                        "generated_at": generated_at,
                        "market_slug": market_slug,
                        "market_name": data.get("market_name"),
                        "market_group_id": data.get("market_group_id"),
                        "period_id": str(period_id),
                        "period_name": periods.get(str(period_id)),
                        "line_type": line_type,
                        "line_name": line_name,
                        "outcome_key": outcome_key,
                        "outcome_name": outcome_name,
                        "bookmaker_id": odd_obj.get("bid"),
                        "bookmaker_name": odd_obj.get("bookie_name"),
                        "bookmaker_slug": odd_obj.get("bookie_slug"),
                        "odds_value": odd_obj.get("odds_value"),
                        "odds_direction": odd_obj.get("odds_direction"),
                        "odds_status": odd_obj.get("odds_status"),
                        "offer_id": str(odd_obj.get("offer_id")) if odd_obj.get("offer_id") is not None else None,
                        "odds_link": odd_obj.get("odds_link"),
                        "raw_payload": json.dumps(payload, separators=(",", ":")),
                    }
                )
    return rows


def _build_match_info_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    generated_at_raw = payload.get("generated_at")
    generated_at = generated_at_raw.replace(" ", "T") + "Z" if isinstance(generated_at_raw, str) else None
    return [
        {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "generated_at": generated_at,
            "match_id": str(data.get("id")) if data.get("id") is not None else None,
            "match_key": str(data.get("match_key")) if data.get("match_key") is not None else None,
            "sport_name": data.get("sport_name"),
            "tournament_name": data.get("league_name"),
            "player1": data.get("ht"),
            "player2": data.get("at"),
            "player1_id": str(data.get("ht_id")) if data.get("ht_id") is not None else None,
            "player2_id": str(data.get("at_id")) if data.get("at_id") is not None else None,
            "starttime": data.get("starttime"),
            "surface": data.get("surface"),
            "venue_name": data.get("venue_name"),
            "venue_city": data.get("venue_city"),
            "raw_payload": json.dumps(data, separators=(",", ":")),
        }
    ]


def _ensure_dataset(client: bigquery.Client, dataset: str, location: str) -> None:
    client.query(f'CREATE SCHEMA IF NOT EXISTS `{dataset}` OPTIONS(location = "{location}")').result()


def _ensure_table(client: bigquery.Client, table_id: str, schema: list[bigquery.SchemaField], description: str) -> None:
    table = bigquery.Table(table_id, schema=schema)
    table.description = description
    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(table)


def _truncate_and_load_json(
    client: bigquery.Client,
    table_id: str,
    rows: Iterable[dict[str, Any]],
    schema: list[bigquery.SchemaField],
) -> int:
    rows = list(rows)
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    if not rows:
        return 0
    job = client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    return len(rows)


def run_workflow(input_dir: Path, dry_run: bool = False) -> dict[str, int]:
    client = _get_client() if not dry_run else None
    dataset = _dataset()
    location = _location()
    if client is not None:
        _ensure_dataset(client, dataset, location)

    odds_schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("generated_at", "TIMESTAMP"),
        bigquery.SchemaField("market_slug", "STRING"),
        bigquery.SchemaField("market_name", "STRING"),
        bigquery.SchemaField("market_group_id", "INT64"),
        bigquery.SchemaField("period_id", "STRING"),
        bigquery.SchemaField("period_name", "STRING"),
        bigquery.SchemaField("line_type", "STRING"),
        bigquery.SchemaField("line_name", "STRING"),
        bigquery.SchemaField("outcome_key", "STRING"),
        bigquery.SchemaField("outcome_name", "STRING"),
        bigquery.SchemaField("bookmaker_id", "INT64"),
        bigquery.SchemaField("bookmaker_name", "STRING"),
        bigquery.SchemaField("bookmaker_slug", "STRING"),
        bigquery.SchemaField("odds_value", "STRING"),
        bigquery.SchemaField("odds_direction", "INT64"),
        bigquery.SchemaField("odds_status", "INT64"),
        bigquery.SchemaField("offer_id", "STRING"),
        bigquery.SchemaField("odds_link", "STRING"),
        bigquery.SchemaField("raw_payload", "JSON"),
    ]
    match_info_schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("generated_at", "TIMESTAMP"),
        bigquery.SchemaField("match_id", "STRING"),
        bigquery.SchemaField("match_key", "STRING"),
        bigquery.SchemaField("sport_name", "STRING"),
        bigquery.SchemaField("tournament_name", "STRING"),
        bigquery.SchemaField("player1", "STRING"),
        bigquery.SchemaField("player2", "STRING"),
        bigquery.SchemaField("player1_id", "STRING"),
        bigquery.SchemaField("player2_id", "STRING"),
        bigquery.SchemaField("starttime", "TIMESTAMP"),
        bigquery.SchemaField("surface", "STRING"),
        bigquery.SchemaField("venue_name", "STRING"),
        bigquery.SchemaField("venue_city", "STRING"),
        bigquery.SchemaField("raw_payload", "JSON"),
    ]

    counts: dict[str, int] = {}

    for file_name, market_slug in MARKET_FILE_TO_SLUG.items():
        market_file = input_dir / file_name
        if not market_file.exists():
            print(f"[atp_per_match] Skipping {file_name} (not found in {input_dir})")
            continue
        payload = _extract_json_payload(market_file.read_text(encoding="utf-8"))
        rows = _normalize_market_rows(payload, market_slug)
        table_name = MARKET_TABLES[market_slug]
        if client is None:
            table_id = table_name
        else:
            table_id = f"{client.project}.{dataset}.{table_name}"
        if dry_run:
            counts[table_name] = len(rows)
        else:
            _ensure_table(client, table_id, odds_schema, f"Oddspedia ATP per-match odds for market {market_slug}")
            counts[table_name] = _truncate_and_load_json(client, table_id, rows, odds_schema)

    match_info_file = input_dir / "match_info"
    if match_info_file.exists():
        match_info_payload = _extract_json_payload(match_info_file.read_text(encoding="utf-8"))
        match_info_table = f"{client.project}.{dataset}.atp_match_info" if client is not None else "atp_match_info"
        if dry_run:
            counts["atp_match_info"] = len(_build_match_info_rows(match_info_payload))
        else:
            _ensure_table(client, match_info_table, match_info_schema, "Oddspedia ATP match info payload snapshot")
            counts["atp_match_info"] = _truncate_and_load_json(
                client,
                match_info_table,
                _build_match_info_rows(match_info_payload),
                match_info_schema,
            )
    else:
        print(f"[atp_per_match] Skipping match_info (not found in {input_dir})")

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load saved Oddspedia ATP per-match captures into BigQuery oddspedia.atp_* tables"
    )
    parser.add_argument("--input-dir", type=Path, default=Path("website_responses/atp"))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate parsing and row counts without BigQuery writes",
    )
    args = parser.parse_args()

    counts = run_workflow(args.input_dir, dry_run=args.dry_run)
    print("Loaded tables:")
    for table, count in sorted(counts.items()):
        print(f"  - {table}: {count} rows")


if __name__ == "__main__":
    main()
