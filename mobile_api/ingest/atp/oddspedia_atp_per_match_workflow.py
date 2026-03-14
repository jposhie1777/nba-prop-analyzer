"""oddspedia_atp_per_match_workflow.py

Reads per-match Oddspedia capture files from website_responses/atp/match_specific/
and loads all odds into a single BigQuery table: oddspedia.atp_odds.

Each file in match_specific/ should be a raw getMatchMaxOddsByGroup API response,
with the filename following the convention:

    {matchId}_{marketGroupId}
    e.g.  9980599_201   9980599_301

Files can be:
  - Pure JSON  (just the response body)
  - DevTools-style captures with headers above the JSON payload

The matchId is extracted from the filename.  The marketGroupId and all odds
data come from the JSON payload.

Included in every row:
  - match_id, market_group_id, market_name, period_id, period_name
  - line_type (main / alternative), line_name (handicap value)
  - outcome_key, outcome_name
  - bookmaker_id, bookmaker_name, bookmaker_slug
  - odds_value, odds_direction, odds_status, offer_id
  - ingested_at

Usage
-----
    python -m mobile_api.ingest.atp.oddspedia_atp_per_match_workflow

    python -m mobile_api.ingest.atp.oddspedia_atp_per_match_workflow --dry-run

    python -m mobile_api.ingest.atp.oddspedia_atp_per_match_workflow \
        --input-dir website_responses/atp/match_specific

Environment variables
---------------------
ODDSPEDIA_DATASET       BigQuery dataset  (default: oddspedia)
ODDSPEDIA_TABLE         BigQuery table    (default: atp_odds)
ODDSPEDIA_BQ_LOCATION   Dataset region    (default: US)
GCP_PROJECT / GOOGLE_CLOUD_PROJECT
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.cloud import bigquery

DATASET = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
TABLE = os.getenv("ODDSPEDIA_TABLE", "atp_odds")
BQ_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")

SCHEMA = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("generated_at", "TIMESTAMP"),
    bigquery.SchemaField("match_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("market_group_id", "INT64"),
    bigquery.SchemaField("market_name", "STRING"),
    bigquery.SchemaField("period_id", "STRING"),
    bigquery.SchemaField("period_name", "STRING"),
    bigquery.SchemaField("line_type", "STRING"),   # "main" | "alternative" | null
    bigquery.SchemaField("line_name", "STRING"),   # handicap label, e.g. "-3.5 Games"
    bigquery.SchemaField("outcome_key", "STRING"),
    bigquery.SchemaField("outcome_name", "STRING"),
    bigquery.SchemaField("bookmaker_id", "INT64"),
    bigquery.SchemaField("bookmaker_name", "STRING"),
    bigquery.SchemaField("bookmaker_slug", "STRING"),
    bigquery.SchemaField("odds_value", "FLOAT64"),
    bigquery.SchemaField("odds_direction", "INT64"),
    bigquery.SchemaField("odds_status", "INT64"),
    bigquery.SchemaField("offer_id", "STRING"),
    bigquery.SchemaField("odds_link", "STRING"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _full_table_id(client: bigquery.Client) -> str:
    return f"{client.project}.{DATASET}.{TABLE}"


def _ensure_table(client: bigquery.Client) -> None:
    table_id = _full_table_id(client)
    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(bigquery.Table(table_id, schema=SCHEMA))
        print(f"[atp_odds] Created {table_id}")


def _extract_match_id_from_header(text: str) -> str | None:
    """Try to pull matchId from a DevTools-style request URL embedded in the file."""
    m = re.search(r"[?&]matchId=(\d+)", text)
    return m.group(1) if m else None


def _extract_json_payload(text: str) -> dict[str, Any]:
    marker = re.search(r"\nResponse\n|\nRESPONSE\n", text)
    if marker:
        start = text.find("{", marker.end())
    else:
        start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in capture")
    return json.loads(text[start:])


def _parse_filename_match_id(filename: str) -> str | None:
    """
    Expect filenames like:
        9980599_201     -> match_id = "9980599"
        9980599_301
        9980599         -> match_id = "9980599"
    """
    stem = Path(filename).stem
    parts = stem.split("_")
    if parts[0].isdigit():
        return parts[0]
    return None


def _safe_float(val: Any) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _normalize_rows(payload: dict[str, Any], match_id: str) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    periods = {str(p["id"]): p["name"] for p in payload.get("periods", []) if "id" in p}
    outcome_names = data.get("outcome_names", [])
    market_group_id = data.get("market_group_id")
    market_name = data.get("market_name")

    generated_at_raw = payload.get("generated_at")
    generated_at = None
    if isinstance(generated_at_raw, str):
        generated_at = generated_at_raw.replace(" ", "T") + "Z"

    ingested_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []

    for period_key, period_obj in data.get("odds", {}).items():
        period_name = periods.get(str(period_key))

        # Two possible shapes:
        # 1) flat: {"odds": {"o1": ..., "o2": ...}, "winning_odd": ...}
        # 2) main+alternative: {"main": {...}, "alternative": [...]}
        if isinstance(period_obj, dict) and "main" in period_obj:
            entries: list[tuple[str | None, dict[str, Any]]] = []
            main = period_obj.get("main")
            if isinstance(main, dict):
                entries.append(("main", main))
            for alt in period_obj.get("alternative", []):
                if isinstance(alt, dict):
                    entries.append(("alternative", alt))
        else:
            entries = [(None, period_obj)]

        for line_type, entry in entries:
            line_name = entry.get("name")
            for outcome_key, odd_obj in (entry.get("odds") or {}).items():
                if not isinstance(odd_obj, dict):
                    continue

                idx = None
                try:
                    idx = int(outcome_key.removeprefix("o")) - 1
                except (ValueError, AttributeError):
                    pass

                outcome_name = (
                    outcome_names[idx]
                    if idx is not None and 0 <= idx < len(outcome_names)
                    else outcome_key
                )

                rows.append(
                    {
                        "ingested_at": ingested_at,
                        "generated_at": generated_at,
                        "match_id": match_id,
                        "market_group_id": market_group_id,
                        "market_name": market_name,
                        "period_id": str(period_key),
                        "period_name": period_name,
                        "line_type": line_type,
                        "line_name": line_name,
                        "outcome_key": outcome_key,
                        "outcome_name": outcome_name,
                        "bookmaker_id": odd_obj.get("bid"),
                        "bookmaker_name": odd_obj.get("bookie_name"),
                        "bookmaker_slug": odd_obj.get("bookie_slug"),
                        "odds_value": _safe_float(odd_obj.get("odds_value")),
                        "odds_direction": odd_obj.get("odds_direction"),
                        "odds_status": odd_obj.get("odds_status"),
                        "offer_id": str(odd_obj["offer_id"]) if odd_obj.get("offer_id") is not None else None,
                        "odds_link": odd_obj.get("odds_link"),
                    }
                )

    return rows


# ── Main workflow ─────────────────────────────────────────────────────────────


def run_workflow(input_dir: Path, dry_run: bool = False) -> int:
    files = sorted(
        f for f in input_dir.iterdir()
        if f.is_file() and not f.name.startswith(".")
    )
    if not files:
        print(f"[atp_odds] No files found in {input_dir}")
        return 0

    all_rows: list[dict[str, Any]] = []
    skipped = 0

    for f in files:
        text = f.read_text(encoding="utf-8")

        # Determine match_id: prefer extracting from request URL in header
        match_id = _extract_match_id_from_header(text) or _parse_filename_match_id(f.name)
        if not match_id:
            print(f"[atp_odds] Skipping {f.name}: cannot determine matchId")
            skipped += 1
            continue

        try:
            payload = _extract_json_payload(text)
        except (ValueError, json.JSONDecodeError) as exc:
            print(f"[atp_odds] Skipping {f.name}: JSON parse error – {exc}")
            skipped += 1
            continue

        rows = _normalize_rows(payload, match_id)
        if not rows:
            print(f"[atp_odds] Skipping {f.name}: no rows parsed")
            skipped += 1
            continue

        market_name = payload.get("data", {}).get("market_name", "?")
        print(f"[atp_odds] {f.name}: matchId={match_id} market={market_name} rows={len(rows)}")
        all_rows.extend(rows)

    print(f"\n[atp_odds] Total rows parsed: {len(all_rows)}  |  files skipped: {skipped}")

    if dry_run or not all_rows:
        print("[atp_odds] Dry-run – no BigQuery writes.")
        return len(all_rows)

    client = _bq_client()
    _ensure_table(client)
    table_id = _full_table_id(client)

    # Truncate then reload
    client.query(f"TRUNCATE TABLE `{table_id}`").result()

    chunk_size = 500
    written = 0
    for i in range(0, len(all_rows), chunk_size):
        chunk = all_rows[i : i + chunk_size]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
        written += len(chunk)

    print(f"[atp_odds] Loaded {written} rows → {table_id}")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest per-match Oddspedia ATP captures into BigQuery oddspedia.atp_odds"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("website_responses/atp/match_specific"),
        help="Directory containing per-match capture files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and count rows without writing to BigQuery",
    )
    args = parser.parse_args()
    run_workflow(args.input_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
