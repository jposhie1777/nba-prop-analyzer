"""
Run once to create the BigQuery dataset and tables.
python setup_bq.py
"""
from google.cloud import bigquery

PROJECT = "graphite-flare-477419-h7"
DATASET = "propfinder"

client = bigquery.Client(project=PROJECT)

# Create dataset
dataset_ref = bigquery.Dataset(f"{PROJECT}.{DATASET}")
dataset_ref.location = "US"
try:
    client.create_dataset(dataset_ref)
    print(f"Created dataset {DATASET}")
except Exception:
    print(f"Dataset {DATASET} already exists")

# ── raw_hit_data ──────────────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.raw_hit_data",
    schema=[
        bigquery.SchemaField("run_date",       "DATE"),
        bigquery.SchemaField("game_pk",        "INTEGER"),
        bigquery.SchemaField("batter_id",      "INTEGER"),
        bigquery.SchemaField("batter_team_id", "INTEGER"),
        bigquery.SchemaField("batter_name",    "STRING"),
        bigquery.SchemaField("bat_side",       "STRING"),
        bigquery.SchemaField("pitcher_id",     "INTEGER"),
        bigquery.SchemaField("pitcher_name",   "STRING"),
        bigquery.SchemaField("pitch_hand",     "STRING"),
        bigquery.SchemaField("pitch_type",     "STRING"),
        bigquery.SchemaField("result",         "STRING"),
        bigquery.SchemaField("launch_speed",   "FLOAT"),
        bigquery.SchemaField("launch_angle",   "FLOAT"),
        bigquery.SchemaField("total_distance", "FLOAT"),
        bigquery.SchemaField("trajectory",     "STRING"),
        bigquery.SchemaField("is_barrel",      "BOOLEAN"),
        bigquery.SchemaField("hr_in_n_parks",  "INTEGER"),
        bigquery.SchemaField("event_date",     "DATE"),
        bigquery.SchemaField("season",         "INTEGER"),
        bigquery.SchemaField("ingested_at",    "TIMESTAMP"),
    ]
), exists_ok=True)
print("Created raw_hit_data")

# ── raw_splits ────────────────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.raw_splits",
    schema=[
        bigquery.SchemaField("run_date",      "DATE"),
        bigquery.SchemaField("batter_id",     "INTEGER"),
        bigquery.SchemaField("batter_name",   "STRING"),
        bigquery.SchemaField("split_code",    "STRING"),   # vl, vr, h, a, etc.
        bigquery.SchemaField("split_name",    "STRING"),
        bigquery.SchemaField("season",        "STRING"),
        bigquery.SchemaField("avg",           "FLOAT"),
        bigquery.SchemaField("obp",           "FLOAT"),
        bigquery.SchemaField("slg",           "FLOAT"),
        bigquery.SchemaField("ops",           "FLOAT"),
        bigquery.SchemaField("home_runs",     "INTEGER"),
        bigquery.SchemaField("at_bats",       "INTEGER"),
        bigquery.SchemaField("hits",          "INTEGER"),
        bigquery.SchemaField("doubles",       "INTEGER"),
        bigquery.SchemaField("triples",       "INTEGER"),
        bigquery.SchemaField("strike_outs",   "INTEGER"),
        bigquery.SchemaField("ingested_at",   "TIMESTAMP"),
    ]
), exists_ok=True)
print("Created raw_splits")

# ── raw_pitcher_matchup ───────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.raw_pitcher_matchup",
    schema=[
        bigquery.SchemaField("run_date",          "DATE"),
        bigquery.SchemaField("game_pk",           "INTEGER"),
        bigquery.SchemaField("pitcher_id",        "INTEGER"),
        bigquery.SchemaField("pitcher_name",      "STRING"),
        bigquery.SchemaField("pitcher_hand",      "STRING"),
        bigquery.SchemaField("opp_team_id",       "INTEGER"),
        bigquery.SchemaField("split",             "STRING"),   # Season/vsLHB/vsRHB
        bigquery.SchemaField("ip",                "FLOAT"),
        bigquery.SchemaField("home_runs",         "INTEGER"),
        bigquery.SchemaField("hr_per_9",          "FLOAT"),
        bigquery.SchemaField("barrel_pct",        "FLOAT"),
        bigquery.SchemaField("hard_hit_pct",      "FLOAT"),
        bigquery.SchemaField("fb_pct",            "FLOAT"),
        bigquery.SchemaField("hr_fb_pct",         "FLOAT"),
        bigquery.SchemaField("whip",              "FLOAT"),
        bigquery.SchemaField("woba",              "FLOAT"),
        bigquery.SchemaField("ingested_at",       "TIMESTAMP"),
    ]
), exists_ok=True)
print("Created raw_pitcher_matchup")

# ── raw_pitch_log ─────────────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.raw_pitch_log",
    schema=[
        bigquery.SchemaField("run_date",       "DATE"),
        bigquery.SchemaField("game_pk",        "INTEGER"),
        bigquery.SchemaField("pitcher_id",     "INTEGER"),
        bigquery.SchemaField("batter_hand",    "STRING"),   # LHB or RHB
        bigquery.SchemaField("pitch_code",     "STRING"),
        bigquery.SchemaField("pitch_name",     "STRING"),
        bigquery.SchemaField("season",         "INTEGER"),
        bigquery.SchemaField("count",          "INTEGER"),
        bigquery.SchemaField("percentage",     "FLOAT"),
        bigquery.SchemaField("home_runs",      "INTEGER"),
        bigquery.SchemaField("woba",           "FLOAT"),
        bigquery.SchemaField("slg",            "FLOAT"),
        bigquery.SchemaField("iso",            "FLOAT"),
        bigquery.SchemaField("whiff",          "FLOAT"),
        bigquery.SchemaField("k_percent",      "FLOAT"),
        bigquery.SchemaField("ingested_at",    "TIMESTAMP"),
    ]
), exists_ok=True)
print("Created raw_pitch_log")

# ── hr_picks_daily ────────────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.hr_picks_daily",
    schema=[
        bigquery.SchemaField("run_date",             "DATE"),
        bigquery.SchemaField("run_timestamp",        "TIMESTAMP"),
        bigquery.SchemaField("game_pk",              "INTEGER"),
        bigquery.SchemaField("home_team",            "STRING"),
        bigquery.SchemaField("away_team",            "STRING"),
        bigquery.SchemaField("batter_id",            "INTEGER"),
        bigquery.SchemaField("batter_name",          "STRING"),
        bigquery.SchemaField("bat_side",             "STRING"),
        bigquery.SchemaField("pitcher_id",           "INTEGER"),
        bigquery.SchemaField("pitcher_name",         "STRING"),
        bigquery.SchemaField("pitcher_hand",         "STRING"),
        # Batter metrics
        bigquery.SchemaField("iso",                  "FLOAT"),
        bigquery.SchemaField("slg",                  "FLOAT"),
        bigquery.SchemaField("l15_ev",               "FLOAT"),
        bigquery.SchemaField("l15_barrel_pct",       "FLOAT"),
        bigquery.SchemaField("season_ev",            "FLOAT"),
        bigquery.SchemaField("season_barrel_pct",    "FLOAT"),
        bigquery.SchemaField("l15_hard_hit_pct",     "FLOAT"),
        bigquery.SchemaField("hr_fb_pct",            "FLOAT"),
        # Pitcher metrics
        bigquery.SchemaField("p_hr9_season",         "FLOAT"),
        bigquery.SchemaField("p_hr9_vs_hand",        "FLOAT"),
        bigquery.SchemaField("p_barrel_pct",         "FLOAT"),
        bigquery.SchemaField("p_hr_fb_pct",          "FLOAT"),
        bigquery.SchemaField("p_hr_vs_hand",         "INTEGER"),
        bigquery.SchemaField("p_fb_pct",             "FLOAT"),
        bigquery.SchemaField("p_hard_hit_pct",       "FLOAT"),
        bigquery.SchemaField("p_iso_allowed",        "FLOAT"),
        # Score + grade
        bigquery.SchemaField("score",                "FLOAT"),
        bigquery.SchemaField("grade",                "STRING"),   # IDEAL/FAVORABLE/AVERAGE/AVOID
        bigquery.SchemaField("why",                  "STRING"),
        bigquery.SchemaField("flags",                "STRING"),   # JSON array of flag strings
    ]
), exists_ok=True)
print("Created hr_picks_daily")

# ── raw_game_weather ──────────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.raw_game_weather",
    schema=[
        bigquery.SchemaField("run_date",          "DATE"),
        bigquery.SchemaField("game_pk",           "INTEGER"),
        bigquery.SchemaField("game_date",         "TIMESTAMP"),
        bigquery.SchemaField("home_team_id",      "INTEGER"),
        bigquery.SchemaField("home_team_name",    "STRING"),
        bigquery.SchemaField("away_team_id",      "INTEGER"),
        bigquery.SchemaField("away_team_name",    "STRING"),
        bigquery.SchemaField("weather_indicator", "STRING"),   # Green / Yellow / Red
        bigquery.SchemaField("game_temp",         "FLOAT"),
        bigquery.SchemaField("wind_speed",        "FLOAT"),
        bigquery.SchemaField("wind_dir",          "INTEGER"),
        bigquery.SchemaField("wind_gust",         "FLOAT"),
        bigquery.SchemaField("precip_prob",       "FLOAT"),
        bigquery.SchemaField("conditions",        "STRING"),
        bigquery.SchemaField("ballpark_name",     "STRING"),
        bigquery.SchemaField("roof_type",         "STRING"),
        bigquery.SchemaField("home_moneyline",    "INTEGER"),
        bigquery.SchemaField("away_moneyline",    "INTEGER"),
        bigquery.SchemaField("over_under",        "FLOAT"),
        bigquery.SchemaField("weather_note",      "STRING"),
        bigquery.SchemaField("ingested_at",       "TIMESTAMP"),
    ]
), exists_ok=True)
print("Created raw_game_weather")

# ── Add new weather + odds columns to hr_picks_daily (idempotent) ─────────────
NEW_HR_PICKS_FIELDS = [
    bigquery.SchemaField("weather_indicator", "STRING"),
    bigquery.SchemaField("game_temp",         "FLOAT"),
    bigquery.SchemaField("wind_speed",        "FLOAT"),
    bigquery.SchemaField("wind_dir",          "INTEGER"),
    bigquery.SchemaField("precip_prob",       "FLOAT"),
    bigquery.SchemaField("ballpark_name",     "STRING"),
    bigquery.SchemaField("roof_type",         "STRING"),
    bigquery.SchemaField("weather_note",      "STRING"),
    bigquery.SchemaField("home_moneyline",    "INTEGER"),
    bigquery.SchemaField("away_moneyline",    "INTEGER"),
    bigquery.SchemaField("over_under",        "FLOAT"),
]

hr_picks_ref = f"{PROJECT}.{DATASET}.hr_picks_daily"
hr_picks_table = client.get_table(hr_picks_ref)
existing_names = {field.name for field in hr_picks_table.schema}
fields_to_add = [f for f in NEW_HR_PICKS_FIELDS if f.name not in existing_names]

if fields_to_add:
    hr_picks_table.schema = list(hr_picks_table.schema) + fields_to_add
    client.update_table(hr_picks_table, ["schema"])
    print(f"Added {len(fields_to_add)} new columns to hr_picks_daily: {[f.name for f in fields_to_add]}")
else:
    print("hr_picks_daily already has all weather/odds columns")

print("\nAll tables created successfully.")