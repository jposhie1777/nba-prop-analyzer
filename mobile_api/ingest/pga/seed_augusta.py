"""
Seed Augusta National Golf Club hole data into BigQuery.

Augusta National's hole layout is well-known and rarely changes.
This script populates the course_holes table with current yardages
as used for the Masters Tournament.

Usage:
    python -m mobile_api.ingest.pga.seed_augusta [--dry-run]
"""

from __future__ import annotations

import argparse
import datetime
import os
from typing import Any, Dict, List

from google.cloud import bigquery

DATASET = os.getenv("PGA_DATASET", "pga_data")

# Augusta National Golf Club — 2024/2025 yardages
# Source: masters.com official yardage card
AUGUSTA_HOLES = [
    {"hole_number": 1, "name": "Tea Olive", "par": 4, "yardage": 445},
    {"hole_number": 2, "name": "Pink Dogwood", "par": 5, "yardage": 575},
    {"hole_number": 3, "name": "Flowering Peach", "par": 4, "yardage": 350},
    {"hole_number": 4, "name": "Flowering Crab Apple", "par": 3, "yardage": 240},
    {"hole_number": 5, "name": "Magnolia", "par": 4, "yardage": 495},
    {"hole_number": 6, "name": "Juniper", "par": 3, "yardage": 180},
    {"hole_number": 7, "name": "Pampas", "par": 4, "yardage": 450},
    {"hole_number": 8, "name": "Yellow Jasmine", "par": 5, "yardage": 570},
    {"hole_number": 9, "name": "Carolina Cherry", "par": 4, "yardage": 460},
    {"hole_number": 10, "name": "Camellia", "par": 4, "yardage": 495},
    {"hole_number": 11, "name": "White Dogwood", "par": 4, "yardage": 520},
    {"hole_number": 12, "name": "Golden Bell", "par": 3, "yardage": 155},
    {"hole_number": 13, "name": "Azalea", "par": 5, "yardage": 510},
    {"hole_number": 14, "name": "Chinese Fir", "par": 4, "yardage": 440},
    {"hole_number": 15, "name": "Firethorn", "par": 5, "yardage": 550},
    {"hole_number": 16, "name": "Redbud", "par": 3, "yardage": 170},
    {"hole_number": 17, "name": "Nandina", "par": 4, "yardage": 440},
    {"hole_number": 18, "name": "Holly", "par": 4, "yardage": 465},
]

AUGUSTA_COURSE = {
    "course_id": 1,  # Internal ID for Augusta
    "name": "Augusta National Golf Club",
    "city": "Augusta",
    "state": "Georgia",
    "country": "USA",
    "par": 72,
    "yardage": "7,545",
    "established": "1933",
    "architect": "Alister MacKenzie / Bobby Jones",
    "fairway_grass": "Rye/Bermuda overseed",
    "rough_grass": "Second cut only",
    "green_grass": "Bentgrass",
}

# Course traits that help with analytics —
# what kind of game does Augusta reward?
AUGUSTA_COURSE_PROFILE = {
    "course_name": "Augusta National Golf Club",
    "tournament_name": "Masters Tournament",
    "primary_traits": [
        "approach_accuracy",       # Elevated, undulating greens demand precision
        "putting",                 # Massive, extremely fast greens with severe slopes
        "distance",               # Long course (7,545 yds) rewards length off the tee
        "shot_shaping",           # Doglegs and wind require draws and fades
        "par5_scoring",           # 4 par 5s are birdie/eagle opportunities
        "short_game",             # Chipping/pitching around contoured greens
    ],
    "secondary_traits": [
        "iron_play",
        "course_management",
        "experience",             # Knowledge of greens/slopes matters enormously
    ],
    "key_stats": [
        "sg_approach",
        "sg_putting",
        "sg_tee_to_green",
        "driving_distance",
        "par5_scoring_avg",
        "gir_pct",
        "proximity_to_hole",
        "scrambling_pct",
    ],
    "notes": (
        "Augusta National rewards elite ball-strikers who can control trajectory "
        "and distance precisely. The greens are among the fastest and most "
        "contoured on Tour — putting and approach accuracy are paramount. "
        "Length off the tee provides significant advantage on the four par 5s "
        "which are primary scoring holes."
    ),
}


def seed(*, dry_run: bool = False) -> Dict[str, Any]:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    client = bigquery.Client(project=project)
    ts = datetime.datetime.utcnow().isoformat()

    # --- Seed course_holes ---
    holes_table = f"{client.project}.{DATASET}.course_holes"
    hole_rows: List[Dict[str, Any]] = []
    for hole in AUGUSTA_HOLES:
        hole_rows.append({
            "run_ts": ts,
            "ingested_at": ts,
            "course_id": AUGUSTA_COURSE["course_id"],
            "course_name": AUGUSTA_COURSE["name"],
            "hole_number": hole["hole_number"],
            "par": hole["par"],
            "yardage": hole["yardage"],
        })

    # --- Seed courses ---
    courses_table = f"{client.project}.{DATASET}.courses"
    course_rows = [{
        "run_ts": ts,
        "ingested_at": ts,
        **AUGUSTA_COURSE,
    }]

    if dry_run:
        print(f"[seed_augusta] DRY RUN — would insert {len(hole_rows)} holes + 1 course row")
        return {"holes": len(hole_rows), "courses": 1, "dry_run": True}

    # Clear existing Augusta data first
    for tbl in [holes_table, courses_table]:
        try:
            client.query(
                f"DELETE FROM `{tbl}` WHERE course_id = @cid",
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("cid", "INT64", 1)]
                ),
            ).result()
        except Exception:
            pass

    errors = client.insert_rows_json(holes_table, hole_rows)
    if errors:
        raise RuntimeError(f"Insert errors (holes): {errors}")
    print(f"[seed_augusta] Inserted {len(hole_rows)} hole rows into {holes_table}")

    errors = client.insert_rows_json(courses_table, course_rows)
    if errors:
        raise RuntimeError(f"Insert errors (courses): {errors}")
    print(f"[seed_augusta] Inserted 1 course row into {courses_table}")

    # --- Seed course profile into a new table ---
    import json
    profile_table = f"{client.project}.{DATASET}.course_profiles"
    profile_schema = [
        bigquery.SchemaField("course_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tournament_name", "STRING"),
        bigquery.SchemaField("primary_traits", "STRING"),
        bigquery.SchemaField("secondary_traits", "STRING"),
        bigquery.SchemaField("key_stats", "STRING"),
        bigquery.SchemaField("notes", "STRING"),
    ]
    bq_table = bigquery.Table(profile_table, schema=profile_schema)
    client.create_table(bq_table, exists_ok=True)

    client.query(
        f"DELETE FROM `{profile_table}` WHERE course_name = @cn",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("cn", "STRING", AUGUSTA_COURSE_PROFILE["course_name"])
            ]
        ),
    ).result()

    profile_row = {
        "course_name": AUGUSTA_COURSE_PROFILE["course_name"],
        "tournament_name": AUGUSTA_COURSE_PROFILE["tournament_name"],
        "primary_traits": json.dumps(AUGUSTA_COURSE_PROFILE["primary_traits"]),
        "secondary_traits": json.dumps(AUGUSTA_COURSE_PROFILE["secondary_traits"]),
        "key_stats": json.dumps(AUGUSTA_COURSE_PROFILE["key_stats"]),
        "notes": AUGUSTA_COURSE_PROFILE["notes"],
    }
    errors = client.insert_rows_json(profile_table, [profile_row])
    if errors:
        raise RuntimeError(f"Insert errors (profile): {errors}")
    print(f"[seed_augusta] Inserted course profile into {profile_table}")

    return {"holes": len(hole_rows), "courses": 1, "profile": 1}


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Seed Augusta National course data.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = seed(dry_run=args.dry_run)
    print(result)


if __name__ == "__main__":
    _cli()
