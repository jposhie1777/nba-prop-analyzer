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
        bigquery.SchemaField("game_date",            "TIMESTAMP"),
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
        # Game context (weather/park/lines)
        bigquery.SchemaField("weather_indicator",    "STRING"),
        bigquery.SchemaField("game_temp",            "FLOAT"),
        bigquery.SchemaField("wind_speed",           "FLOAT"),
        bigquery.SchemaField("wind_dir",             "INTEGER"),
        bigquery.SchemaField("precip_prob",          "FLOAT"),
        bigquery.SchemaField("ballpark_name",        "STRING"),
        bigquery.SchemaField("roof_type",            "STRING"),
        bigquery.SchemaField("weather_note",         "STRING"),
        bigquery.SchemaField("home_moneyline",       "INTEGER"),
        bigquery.SchemaField("away_moneyline",       "INTEGER"),
        bigquery.SchemaField("over_under",           "FLOAT"),
        # HR 1+ odds + links
        bigquery.SchemaField("hr_odds_best_price",   "INTEGER"),
        bigquery.SchemaField("hr_odds_best_book",    "STRING"),
        bigquery.SchemaField("deep_link_desktop",    "STRING"),
        bigquery.SchemaField("deep_link_ios",        "STRING"),
        # Sportsbook deep-link parsed fields (HR 1+)
        bigquery.SchemaField("dk_outcome_code",      "STRING"),
        bigquery.SchemaField("dk_event_id",          "STRING"),
        bigquery.SchemaField("fd_market_id",         "STRING"),
        bigquery.SchemaField("fd_selection_id",      "STRING"),
        # Weather conditions
        bigquery.SchemaField("conditions",           "STRING"),   # e.g. "Clear", "Partly Cloudy", "Rain"
        # Batter vs Pitcher career stats
        bigquery.SchemaField("bvp_ab",               "INTEGER"),
        bigquery.SchemaField("bvp_hits",             "INTEGER"),
        bigquery.SchemaField("bvp_hr",               "INTEGER"),
        # Team abbreviations per player
        bigquery.SchemaField("batter_team",          "STRING"),
        bigquery.SchemaField("pitcher_team",         "STRING"),
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

# ── k_picks_daily ─────────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.k_picks_daily",
    schema=[
        bigquery.SchemaField("run_date",             "DATE"),
        bigquery.SchemaField("run_timestamp",        "TIMESTAMP"),
        bigquery.SchemaField("game_pk",              "INTEGER"),
        bigquery.SchemaField("game_date",            "TIMESTAMP"),
        bigquery.SchemaField("pitcher_id",           "INTEGER"),
        bigquery.SchemaField("pitcher_name",         "STRING"),
        bigquery.SchemaField("pitcher_hand",         "STRING"),
        bigquery.SchemaField("team_code",            "STRING"),
        bigquery.SchemaField("opp_team_code",        "STRING"),
        # K prop line
        bigquery.SchemaField("line",                 "FLOAT"),
        bigquery.SchemaField("side",                 "STRING"),     # OVER or UNDER
        bigquery.SchemaField("best_price",           "INTEGER"),
        bigquery.SchemaField("best_book",            "STRING"),
        bigquery.SchemaField("deep_link_desktop",    "STRING"),
        bigquery.SchemaField("deep_link_ios",        "STRING"),
        # Pitcher K metrics
        bigquery.SchemaField("k_per_9",              "FLOAT"),
        bigquery.SchemaField("k_pct",                "FLOAT"),
        bigquery.SchemaField("season_k_per_9",       "FLOAT"),
        bigquery.SchemaField("ip",                   "FLOAT"),
        bigquery.SchemaField("batters_faced",        "INTEGER"),
        bigquery.SchemaField("strike_pct",           "FLOAT"),
        # Pitch arsenal K power
        bigquery.SchemaField("arsenal_whiff_avg",    "FLOAT"),
        bigquery.SchemaField("arsenal_k_pct_avg",    "FLOAT"),
        bigquery.SchemaField("num_high_whiff_pitches","INTEGER"),
        # PropFinder signals
        bigquery.SchemaField("pf_rating",            "FLOAT"),
        bigquery.SchemaField("avg_l10",              "FLOAT"),
        bigquery.SchemaField("avg_home_away",        "FLOAT"),
        bigquery.SchemaField("avg_vs_opponent",      "FLOAT"),
        bigquery.SchemaField("hit_rate_l10",         "STRING"),
        bigquery.SchemaField("hit_rate_season",      "STRING"),
        bigquery.SchemaField("hit_rate_vs_team",     "STRING"),
        bigquery.SchemaField("streak",               "INTEGER"),
        # Team vulnerability
        bigquery.SchemaField("opp_team_k_rank",      "INTEGER"),
        bigquery.SchemaField("opp_team_k_total",     "INTEGER"),
        # Game context
        bigquery.SchemaField("game_total",           "FLOAT"),
        bigquery.SchemaField("ballpark_name",        "STRING"),
        # Score + grade
        bigquery.SchemaField("score",                "FLOAT"),
        bigquery.SchemaField("grade",                "STRING"),
        bigquery.SchemaField("why",                  "STRING"),
        bigquery.SchemaField("flags",                "STRING"),
        # Actual result (filled by analytics after game)
        bigquery.SchemaField("actual_k",             "INTEGER"),
        bigquery.SchemaField("hit",                  "BOOLEAN"),
    ]
), exists_ok=True)
print("Created k_picks_daily")

# ── k_model_weights ──────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.k_model_weights",
    schema=[
        bigquery.SchemaField("run_date",      "DATE"),
        bigquery.SchemaField("factor",        "STRING"),
        bigquery.SchemaField("weight",        "FLOAT"),
        bigquery.SchemaField("sample_size",   "INTEGER"),
        bigquery.SchemaField("correlation",   "FLOAT"),
        bigquery.SchemaField("hit_rate_pct",  "FLOAT"),
        bigquery.SchemaField("updated_at",    "TIMESTAMP"),
    ]
), exists_ok=True)
print("Created k_model_weights")

# ── hit_picks_daily ──────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.hit_picks_daily",
    schema=[
        bigquery.SchemaField("run_date",             "DATE"),
        bigquery.SchemaField("run_timestamp",        "TIMESTAMP"),
        bigquery.SchemaField("game_pk",              "INTEGER"),
        bigquery.SchemaField("game_date",            "TIMESTAMP"),
        bigquery.SchemaField("batter_id",            "INTEGER"),
        bigquery.SchemaField("batter_name",          "STRING"),
        bigquery.SchemaField("bat_side",             "STRING"),
        bigquery.SchemaField("pitcher_id",           "INTEGER"),
        bigquery.SchemaField("pitcher_name",         "STRING"),
        bigquery.SchemaField("pitcher_hand",         "STRING"),
        bigquery.SchemaField("team_code",            "STRING"),
        bigquery.SchemaField("opp_team_code",        "STRING"),
        bigquery.SchemaField("line",                 "FLOAT"),
        bigquery.SchemaField("side",                 "STRING"),
        bigquery.SchemaField("best_price",           "INTEGER"),
        bigquery.SchemaField("best_book",            "STRING"),
        bigquery.SchemaField("deep_link_desktop",    "STRING"),
        bigquery.SchemaField("deep_link_ios",        "STRING"),
        bigquery.SchemaField("dk_outcome_code",      "STRING"),
        bigquery.SchemaField("dk_event_id",          "STRING"),
        bigquery.SchemaField("fd_market_id",         "STRING"),
        bigquery.SchemaField("fd_selection_id",       "STRING"),
        # Batter contact metrics
        bigquery.SchemaField("batting_avg_vs_hand",  "FLOAT"),
        bigquery.SchemaField("contact_rate",         "FLOAT"),
        bigquery.SchemaField("l15_hit_rate",         "FLOAT"),
        bigquery.SchemaField("l15_avg",              "FLOAT"),
        bigquery.SchemaField("hard_hit_pct",         "FLOAT"),
        bigquery.SchemaField("ground_ball_pct",      "FLOAT"),
        bigquery.SchemaField("line_drive_pct",       "FLOAT"),
        # Pitcher vulnerability metrics
        bigquery.SchemaField("p_whip",               "FLOAT"),
        bigquery.SchemaField("p_k_rate",             "FLOAT"),
        bigquery.SchemaField("p_woba_allowed",       "FLOAT"),
        bigquery.SchemaField("p_hard_hit_allowed",   "FLOAT"),
        bigquery.SchemaField("p_hits_per_9",         "FLOAT"),
        # Matchup
        bigquery.SchemaField("bvp_ab",               "INTEGER"),
        bigquery.SchemaField("bvp_hits",             "INTEGER"),
        bigquery.SchemaField("bvp_avg",              "FLOAT"),
        bigquery.SchemaField("platoon_edge",         "BOOLEAN"),
        # PropFinder signals
        bigquery.SchemaField("pf_rating",            "FLOAT"),
        bigquery.SchemaField("matchup_value",        "FLOAT"),
        bigquery.SchemaField("avg_l10",              "FLOAT"),
        bigquery.SchemaField("avg_home_away",        "FLOAT"),
        bigquery.SchemaField("avg_vs_opponent",      "FLOAT"),
        bigquery.SchemaField("hit_rate_l10",         "STRING"),
        bigquery.SchemaField("hit_rate_season",      "STRING"),
        bigquery.SchemaField("hit_rate_vs_team",     "STRING"),
        bigquery.SchemaField("streak",               "INTEGER"),
        # Context
        bigquery.SchemaField("game_total",           "FLOAT"),
        bigquery.SchemaField("ballpark_name",        "STRING"),
        # Weak spot
        bigquery.SchemaField("batting_order_pos",    "INTEGER"),
        bigquery.SchemaField("ws_batting_order",     "INTEGER"),
        bigquery.SchemaField("ws_at_bats",           "INTEGER"),
        bigquery.SchemaField("ws_hits",              "INTEGER"),
        bigquery.SchemaField("ws_avg",               "FLOAT"),
        # Output
        bigquery.SchemaField("score",                "FLOAT"),
        bigquery.SchemaField("grade",                "STRING"),
        bigquery.SchemaField("why",                  "STRING"),
        bigquery.SchemaField("flags",                "STRING"),
        # Actuals (filled by hit_analytics after games)
        bigquery.SchemaField("actual_hits",          "INTEGER"),
        bigquery.SchemaField("hit",                  "BOOLEAN"),
    ]
), exists_ok=True)
print("Created hit_picks_daily")

# ── hit_model_weights ────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.hit_model_weights",
    schema=[
        bigquery.SchemaField("run_date",      "DATE"),
        bigquery.SchemaField("factor",        "STRING"),
        bigquery.SchemaField("weight",        "FLOAT"),
        bigquery.SchemaField("baseline",      "FLOAT"),
        bigquery.SchemaField("sample_size",   "INTEGER"),
        bigquery.SchemaField("correlation",   "FLOAT"),
        bigquery.SchemaField("hit_rate_pct",  "FLOAT"),
        bigquery.SchemaField("updated_at",    "TIMESTAMP"),
    ]
), exists_ok=True)
print("Created hit_model_weights")

# ── Add DK/FD columns to hit_picks_daily (idempotent) ───────────────────
HIT_PICKS_NEW_FIELDS = [
    bigquery.SchemaField("dk_outcome_code",  "STRING"),
    bigquery.SchemaField("dk_event_id",      "STRING"),
    bigquery.SchemaField("fd_market_id",     "STRING"),
    bigquery.SchemaField("fd_selection_id",  "STRING"),
    bigquery.SchemaField("batting_order_pos","INTEGER"),
    bigquery.SchemaField("ws_batting_order", "INTEGER"),
    bigquery.SchemaField("ws_at_bats",       "INTEGER"),
    bigquery.SchemaField("ws_hits",          "INTEGER"),
    bigquery.SchemaField("ws_avg",           "FLOAT"),
]
try:
    hit_tbl = client.get_table(f"{PROJECT}.{DATASET}.hit_picks_daily")
    hit_existing = {f.name for f in hit_tbl.schema}
    hit_new = [f for f in HIT_PICKS_NEW_FIELDS if f.name not in hit_existing]
    if hit_new:
        hit_tbl.schema = list(hit_tbl.schema) + hit_new
        client.update_table(hit_tbl, ["schema"])
        print(f"Added {len(hit_new)} DK/FD columns to hit_picks_daily")
    else:
        print("hit_picks_daily already has DK/FD columns")
except Exception as exc:
    print(f"Could not update hit_picks_daily schema: {exc}")

# ── hr_model_weights ─────────────────────────────────────────────────────
client.create_table(bigquery.Table(
    f"{PROJECT}.{DATASET}.hr_model_weights",
    schema=[
        bigquery.SchemaField("run_date",      "DATE"),
        bigquery.SchemaField("factor",        "STRING"),
        bigquery.SchemaField("weight",        "FLOAT"),
        bigquery.SchemaField("baseline",      "FLOAT"),
        bigquery.SchemaField("sample_size",   "INTEGER"),
        bigquery.SchemaField("correlation",   "FLOAT"),
        bigquery.SchemaField("hit_rate_pct",  "FLOAT"),
        bigquery.SchemaField("updated_at",    "TIMESTAMP"),
    ]
), exists_ok=True)
print("Created hr_model_weights")

# ── Add actual_hr + hit columns to hr_picks_daily (idempotent) ──────────
HR_RESULT_FIELDS = [
    bigquery.SchemaField("actual_hr",  "INTEGER"),
    bigquery.SchemaField("hit",        "BOOLEAN"),
]
hr_picks_table2 = client.get_table(hr_picks_ref)
existing_names2 = {field.name for field in hr_picks_table2.schema}
result_fields_to_add = [f for f in HR_RESULT_FIELDS if f.name not in existing_names2]

if result_fields_to_add:
    hr_picks_table2.schema = list(hr_picks_table2.schema) + result_fields_to_add
    client.update_table(hr_picks_table2, ["schema"])
    print(f"Added {len(result_fields_to_add)} result columns to hr_picks_daily: {[f.name for f in result_fields_to_add]}")
else:
    print("hr_picks_daily already has actual_hr/hit columns")

# ── Add game_date column to all picks tables (idempotent) ────────────────
GAME_DATE_FIELD = bigquery.SchemaField("game_date", "TIMESTAMP")
for tbl_name in ["hr_picks_daily", "k_picks_daily", "hit_picks_daily"]:
    try:
        tbl_ref = client.get_table(f"{PROJECT}.{DATASET}.{tbl_name}")
        if "game_date" not in {f.name for f in tbl_ref.schema}:
            tbl_ref.schema = list(tbl_ref.schema) + [GAME_DATE_FIELD]
            client.update_table(tbl_ref, ["schema"])
            print(f"Added game_date column to {tbl_name}")
        else:
            print(f"{tbl_name} already has game_date column")
    except Exception as exc:
        print(f"Could not update {tbl_name} schema: {exc}")

print("\nAll tables created successfully.")