#mobile_api/ingest/mls/oddspedia_mls_bq_workflow.py
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from google.cloud import bigquery

MARKET_FILE_TO_SLUG = {
    "1x2": "1x2",
    "btts": "btts",
    "draw_no_bet": "draw_no_bet",
    "double_chance": "double_chance",
    "euro_handicap": "european_handicap",
    "total_corners": "total_corners",
}

MARKET_TABLES = {
    "1x2": "mls_odds_1x2",
    "btts": "mls_odds_btts",
    "draw_no_bet": "mls_odds_draw_no_bet",
    "double_chance": "mls_odds_double_chance",
    "european_handicap": "mls_odds_european_handicap",
    "total_corners": "mls_odds_total_corners",
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


def _extract_statistics_tokens(text: str) -> list[str]:
    marker = text.find("Response")
    if marker == -1:
        return []
    array_start = text.find("[", marker)
    array_end = text.rfind("]")
    if array_start == -1 or array_end == -1 or array_end <= array_start:
        return []
    try:
        values = json.loads(text[array_start : array_end + 1])
    except json.JSONDecodeError:
        return []
    return [v for v in values if isinstance(v, str)]


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
            "league_name": data.get("league_name"),
            "home_team": data.get("ht"),
            "away_team": data.get("at"),
            "starttime": data.get("starttime"),
            "venue_name": data.get("venue_name"),
            "venue_city": data.get("venue_city"),
            "raw_payload": json.dumps(data, separators=(",", ":")),
        }
    ]


def _build_match_keys_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    generated_at_raw = payload.get("generated_at")
    generated_at = generated_at_raw.replace(" ", "T") + "Z" if isinstance(generated_at_raw, str) else None
    match_id = str(data.get("id")) if data.get("id") is not None else None
    rows = []
    for idx, item in enumerate(data.get("match_keys", []), start=1):
        rows.append(
            {
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "generated_at": generated_at,
                "match_id": match_id,
                "insight_rank": idx,
                "statement": item.get("statement"),
                "teams_json": json.dumps(item.get("teams", [])),
            }
        )
    return rows


def _build_statistics_rows(text: str) -> list[dict[str, Any]]:
    tokens = _extract_statistics_tokens(text)
    now = datetime.now(timezone.utc).isoformat()
    return [
        {
            "ingested_at": now,
            "token_index": i,
            "token": token,
        }
        for i, token in enumerate(tokens)
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
        bigquery.SchemaField("league_name", "STRING"),
        bigquery.SchemaField("home_team", "STRING"),
        bigquery.SchemaField("away_team", "STRING"),
        bigquery.SchemaField("starttime", "TIMESTAMP"),
        bigquery.SchemaField("venue_name", "STRING"),
        bigquery.SchemaField("venue_city", "STRING"),
        bigquery.SchemaField("raw_payload", "JSON"),
    ]
    match_keys_schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("scraped_date", "DATE"),
        bigquery.SchemaField("generated_at", "TIMESTAMP"),
        bigquery.SchemaField("match_id", "STRING"),
        bigquery.SchemaField("insight_rank", "INT64"),
        bigquery.SchemaField("statement", "STRING"),
        bigquery.SchemaField("teams_json", "JSON"),
    ]
    stats_schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("token_index", "INT64"),
        bigquery.SchemaField("token", "STRING"),
    ]

    counts: dict[str, int] = {}

    for file_name, market_slug in MARKET_FILE_TO_SLUG.items():
        payload = _extract_json_payload((input_dir / file_name).read_text(encoding="utf-8"))
        rows = _normalize_market_rows(payload, market_slug)
        table_name = MARKET_TABLES[market_slug]
        if client is None:
            table_id = table_name
        else:
            table_id = f"{client.project}.{dataset}.{table_name}"
        if dry_run:
            counts[table_name] = len(rows)
        else:
            _ensure_table(client, table_id, odds_schema, f"Oddspedia MLS odds rows for market {market_slug}")
            counts[table_name] = _truncate_and_load_json(client, table_id, rows, odds_schema)

    match_info_payload = _extract_json_payload((input_dir / "match_info").read_text(encoding="utf-8"))

    match_info_table = f"{client.project}.{dataset}.mls_match_info" if client is not None else "mls_match_info"
    if dry_run:
        counts["mls_match_info"] = len(_build_match_info_rows(match_info_payload))
    else:
        _ensure_table(client, match_info_table, match_info_schema, "Oddspedia MLS match info payload snapshot")
        counts["mls_match_info"] = _truncate_and_load_json(
        client,
        match_info_table,
        _build_match_info_rows(match_info_payload),
        match_info_schema,
    )

    match_keys_table = f"{client.project}.{dataset}.mls_match_keys" if client is not None else "mls_match_keys"
    if dry_run:
        counts["mls_match_keys"] = len(_build_match_keys_rows(match_info_payload))
    else:
        _ensure_table(client, match_keys_table, match_keys_schema, "Oddspedia MLS match key insights")
        counts["mls_match_keys"] = _truncate_and_load_json(
        client,
        match_keys_table,
        _build_match_keys_rows(match_info_payload),
        match_keys_schema,
    )

    stats_text = (input_dir / "statistics_extract").read_text(encoding="utf-8")
    stats_table = f"{client.project}.{dataset}.mls_statistics_tokens" if client is not None else "mls_statistics_tokens"
    if dry_run:
        counts["mls_statistics_tokens"] = len(_build_statistics_rows(stats_text))
    else:
        _ensure_table(client, stats_table, stats_schema, "Oddspedia MLS statistics extract token stream")
        counts["mls_statistics_tokens"] = _truncate_and_load_json(
        client,
        stats_table,
        _build_statistics_rows(stats_text),
        stats_schema,
    )

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Load saved Oddspedia MLS captures into BigQuery oddspedia.mls_* tables")
    parser.add_argument("--input-dir", type=Path, default=Path("website_responses/mls"))
    parser.add_argument("--dry-run", action="store_true", help="Validate parsing and row counts without BigQuery writes")
    args = parser.parse_args()

    counts = run_workflow(args.input_dir, dry_run=args.dry_run)
    print("Loaded tables:")
    for table, count in sorted(counts.items()):
        print(f"  - {table}: {count} rows")


if __name__ == "__main__":
    main()
