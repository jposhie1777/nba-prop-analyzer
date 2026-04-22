"""
Create BigQuery dataset + tables for the NBA pipeline.
Idempotent — safe to re-run.
"""

from google.cloud import bigquery
from config import PROJECT, DATASET

client = bigquery.Client(project=PROJECT)

ds_ref = bigquery.Dataset(f"{PROJECT}.{DATASET}")
ds_ref.location = "US"
try:
    client.create_dataset(ds_ref)
    print(f"Created dataset {DATASET}")
except Exception:
    print(f"Dataset {DATASET} already exists")


def _create(name, schema):
    client.create_table(
        bigquery.Table(f"{PROJECT}.{DATASET}.{name}", schema=schema),
        exists_ok=True,
    )
    print(f"  {name}")


def _sf(name, typ, mode="NULLABLE"):
    return bigquery.SchemaField(name, typ, mode=mode)


print("Creating tables...")

# ── raw_nba_props ─────────────────────────────────────────────────────────────
_create("raw_nba_props", [
    _sf("run_date",          "DATE"),
    _sf("prop_id",           "STRING"),
    _sf("player_id",         "STRING"),
    _sf("game_id",           "STRING"),
    _sf("team_id",           "STRING"),
    _sf("opp_team_id",       "STRING"),
    _sf("player_name",       "STRING"),
    _sf("team_code",         "STRING"),
    _sf("opp_team_code",     "STRING"),
    _sf("position",          "STRING"),
    _sf("injury_status",     "STRING"),
    _sf("is_home",           "BOOLEAN"),
    _sf("category",          "STRING"),
    _sf("line",              "FLOAT"),
    _sf("over_under",        "STRING"),
    _sf("is_alternate",      "BOOLEAN"),
    _sf("pf_rating",         "FLOAT"),
    _sf("matchup_rank",      "INTEGER"),
    _sf("matchup_value",     "FLOAT"),
    _sf("matchup_label",     "STRING"),
    _sf("hit_rate_l5",       "STRING"),
    _sf("hit_rate_l10",      "STRING"),
    _sf("hit_rate_l20",      "STRING"),
    _sf("hit_rate_season",   "STRING"),
    _sf("hit_rate_last_season", "STRING"),
    _sf("hit_rate_vs_team",  "STRING"),
    _sf("streak",            "STRING"),
    _sf("avg_l10",           "FLOAT"),
    _sf("avg_home_away",     "FLOAT"),
    _sf("avg_vs_opponent",   "FLOAT"),
    # Best market (DK or FD only)
    _sf("best_book",         "STRING"),
    _sf("best_price",        "INTEGER"),
    _sf("best_line",         "FLOAT"),
    # DK deep link parts
    _sf("dk_price",          "INTEGER"),
    _sf("dk_deep_link",      "STRING"),
    _sf("dk_outcome_code",   "STRING"),
    _sf("dk_event_id",       "STRING"),
    # FD deep link parts
    _sf("fd_price",          "INTEGER"),
    _sf("fd_deep_link",      "STRING"),
    _sf("fd_market_id",      "STRING"),
    _sf("fd_selection_id",   "STRING"),
    _sf("ingested_at",       "TIMESTAMP"),
])

# ── raw_nba_games ─────────────────────────────────────────────────────────────
_create("raw_nba_games", [
    _sf("run_date",              "DATE"),
    _sf("game_id",               "STRING"),
    _sf("game_date",             "TIMESTAMP"),
    _sf("home_team_id",          "STRING"),
    _sf("home_team_code",        "STRING"),
    _sf("home_team_name",        "STRING"),
    _sf("away_team_id",          "STRING"),
    _sf("away_team_code",        "STRING"),
    _sf("away_team_name",        "STRING"),
    _sf("home_ml",               "STRING"),
    _sf("away_ml",               "STRING"),
    _sf("home_spread_line",      "STRING"),
    _sf("home_spread_odds",      "STRING"),
    _sf("away_spread_line",      "STRING"),
    _sf("away_spread_odds",      "STRING"),
    _sf("total_line",            "FLOAT"),
    _sf("ingested_at",           "TIMESTAMP"),
])

# ── raw_nba_splits (season-level advanced stats) ──────────────────────────────
_create("raw_nba_splits", [
    _sf("run_date",              "DATE"),
    _sf("player_id",             "STRING"),
    _sf("player_name",           "STRING"),
    _sf("position",              "STRING"),
    _sf("season_year",           "INTEGER"),
    _sf("season_type",           "STRING"),
    _sf("games_played",          "INTEGER"),
    _sf("games_started",         "INTEGER"),
    _sf("minutes",               "FLOAT"),
    _sf("points",                "INTEGER"),
    _sf("rebounds",              "INTEGER"),
    _sf("offensive_rebounds",    "INTEGER"),
    _sf("defensive_rebounds",    "INTEGER"),
    _sf("assists",               "INTEGER"),
    _sf("steals",                "INTEGER"),
    _sf("blocks",                "INTEGER"),
    _sf("turnovers",             "INTEGER"),
    _sf("field_goals_made",      "INTEGER"),
    _sf("field_goals_att",       "INTEGER"),
    _sf("field_goals_pct",       "FLOAT"),
    _sf("three_points_made",     "INTEGER"),
    _sf("three_points_att",      "INTEGER"),
    _sf("three_points_pct",      "FLOAT"),
    _sf("free_throws_made",      "INTEGER"),
    _sf("free_throws_att",       "INTEGER"),
    _sf("free_throws_pct",       "FLOAT"),
    _sf("two_points_made",       "INTEGER"),
    _sf("two_points_att",        "INTEGER"),
    _sf("two_points_pct",        "FLOAT"),
    _sf("usage_pct",             "FLOAT"),
    _sf("true_shooting_pct",     "FLOAT"),
    _sf("effective_fg_pct",      "FLOAT"),
    _sf("assists_turnover_ratio","FLOAT"),
    _sf("efficiency",            "INTEGER"),
    _sf("points_in_paint",       "INTEGER"),
    _sf("fast_break_pts",        "INTEGER"),
    _sf("second_chance_pts",     "INTEGER"),
    _sf("plus",                  "INTEGER"),
    _sf("minus",                 "INTEGER"),
    _sf("double_doubles",        "INTEGER"),
    _sf("fouls_drawn",           "INTEGER"),
    _sf("fg_at_rim_made",        "INTEGER"),
    _sf("fg_at_rim_att",         "INTEGER"),
    _sf("fg_at_rim_pct",         "FLOAT"),
    _sf("fg_midrange_made",      "INTEGER"),
    _sf("fg_midrange_att",       "INTEGER"),
    _sf("fg_midrange_pct",       "FLOAT"),
    _sf("ingested_at",           "TIMESTAMP"),
])

# ── raw_nba_game_logs (per-game player stats) ─────────────────────────────────
_create("raw_nba_game_logs", [
    _sf("run_date",              "DATE"),
    _sf("player_id",             "STRING"),
    _sf("player_name",           "STRING"),
    _sf("position",              "STRING"),
    _sf("team_id",               "STRING"),
    _sf("team_code",             "STRING"),
    _sf("opp_team_id",           "STRING"),
    _sf("opp_team_code",         "STRING"),
    _sf("game_id",               "STRING"),
    _sf("game_date",             "DATE"),
    _sf("season",                "INTEGER"),
    _sf("season_type",           "STRING"),
    _sf("is_home",               "BOOLEAN"),
    _sf("is_win",                "BOOLEAN"),
    _sf("point_differential",    "INTEGER"),
    _sf("minutes",               "FLOAT"),
    _sf("points",                "INTEGER"),
    _sf("rebounds",              "INTEGER"),
    _sf("offensive_rebounds",    "INTEGER"),
    _sf("defensive_rebounds",    "INTEGER"),
    _sf("assists",               "INTEGER"),
    _sf("steals",                "INTEGER"),
    _sf("blocks",                "INTEGER"),
    _sf("turnovers",             "INTEGER"),
    _sf("field_goals_made",      "INTEGER"),
    _sf("field_goals_att",       "INTEGER"),
    _sf("field_goals_pct",       "FLOAT"),
    _sf("three_points_made",     "INTEGER"),
    _sf("three_points_att",      "INTEGER"),
    _sf("two_points_made",       "INTEGER"),
    _sf("two_points_att",        "INTEGER"),
    _sf("free_throws_made",      "INTEGER"),
    _sf("free_throws_att",       "INTEGER"),
    _sf("steals_blocks",         "INTEGER"),
    _sf("points_rebounds_assists","INTEGER"),
    _sf("points_assists",        "INTEGER"),
    _sf("rebound_assists",       "INTEGER"),
    _sf("points_rebounds",       "INTEGER"),
    _sf("usage_pct",             "FLOAT"),
    _sf("efficiency",            "INTEGER"),
    _sf("contested_rebounds",    "INTEGER"),
    _sf("potential_assists",     "INTEGER"),
    _sf("effective_fg_pct",      "FLOAT"),
    _sf("ingested_at",           "TIMESTAMP"),
])

# ── nba_picks_daily (scored props — one table, partitioned by category) ───────
_create("nba_picks_daily", [
    _sf("run_date",          "DATE"),
    _sf("prop_id",           "STRING"),
    _sf("player_id",         "STRING"),
    _sf("game_id",           "STRING"),
    _sf("player_name",       "STRING"),
    _sf("team_code",         "STRING"),
    _sf("opp_team_code",     "STRING"),
    _sf("position",          "STRING"),
    _sf("is_home",           "BOOLEAN"),
    _sf("category",          "STRING"),
    _sf("line",              "FLOAT"),
    _sf("over_under",        "STRING"),
    _sf("pulse_score",       "FLOAT"),
    _sf("grade",             "STRING"),
    _sf("reasoning",         "STRING"),
    # Factor scores for learning
    _sf("f_pf_rating",       "FLOAT"),
    _sf("f_matchup",         "FLOAT"),
    _sf("f_hit_rate",        "FLOAT"),
    _sf("f_recent_avg",      "FLOAT"),
    _sf("f_home_away",       "FLOAT"),
    _sf("f_vs_opponent",     "FLOAT"),
    _sf("f_usage",           "FLOAT"),
    _sf("f_game_total",      "FLOAT"),
    _sf("f_spread_context",  "FLOAT"),
    _sf("f_consistency",     "FLOAT"),
    # Best book info
    _sf("best_book",         "STRING"),
    _sf("best_price",        "INTEGER"),
    _sf("dk_price",          "INTEGER"),
    _sf("dk_deep_link",      "STRING"),
    _sf("dk_outcome_code",   "STRING"),
    _sf("dk_event_id",       "STRING"),
    _sf("fd_price",          "INTEGER"),
    _sf("fd_deep_link",      "STRING"),
    _sf("fd_market_id",      "STRING"),
    _sf("fd_selection_id",   "STRING"),
    # Outcome (filled by analytics)
    _sf("actual_value",      "FLOAT"),
    _sf("is_hit",            "BOOLEAN"),
    _sf("scored_at",         "TIMESTAMP"),
])

# ── nba_game_picks (game line predictions) ────────────────────────────────────
_create("nba_game_picks", [
    _sf("run_date",              "DATE"),
    _sf("game_id",               "STRING"),
    _sf("home_team_code",        "STRING"),
    _sf("away_team_code",        "STRING"),
    _sf("home_team_name",        "STRING"),
    _sf("away_team_name",        "STRING"),
    _sf("game_date",             "TIMESTAMP"),
    # Moneyline
    _sf("ml_pick",               "STRING"),
    _sf("ml_odds",               "STRING"),
    _sf("ml_pulse",              "FLOAT"),
    _sf("ml_grade",              "STRING"),
    _sf("ml_reasoning",          "STRING"),
    # Spread
    _sf("spread_pick",           "STRING"),
    _sf("spread_line",           "STRING"),
    _sf("spread_odds",           "STRING"),
    _sf("spread_pulse",          "FLOAT"),
    _sf("spread_grade",          "STRING"),
    _sf("spread_reasoning",      "STRING"),
    # Total
    _sf("total_pick",            "STRING"),   # over/under
    _sf("total_line",            "FLOAT"),
    _sf("total_pulse",           "FLOAT"),
    _sf("total_grade",           "STRING"),
    _sf("total_reasoning",       "STRING"),
    # Outcomes
    _sf("home_score",            "INTEGER"),
    _sf("away_score",            "INTEGER"),
    _sf("ml_hit",                "BOOLEAN"),
    _sf("spread_hit",            "BOOLEAN"),
    _sf("total_hit",             "BOOLEAN"),
    _sf("scored_at",             "TIMESTAMP"),
])

# ── nba_model_weights (learned factor weights) ───────────────────────────────
_create("nba_model_weights", [
    _sf("category",          "STRING"),
    _sf("factor_name",       "STRING"),
    _sf("weight",            "FLOAT"),
    _sf("sample_size",       "INTEGER"),
    _sf("win_rate",          "FLOAT"),
    _sf("updated_at",        "TIMESTAMP"),
])

# ── nba_league_outcomes (all player game results for learning) ────────────────
_create("nba_league_outcomes", [
    _sf("game_date",         "DATE"),
    _sf("game_id",           "STRING"),
    _sf("player_id",         "STRING"),
    _sf("player_name",       "STRING"),
    _sf("team_code",         "STRING"),
    _sf("minutes",           "FLOAT"),
    _sf("points",            "INTEGER"),
    _sf("rebounds",          "INTEGER"),
    _sf("assists",           "INTEGER"),
    _sf("three_points_made", "INTEGER"),
    _sf("steals",            "INTEGER"),
    _sf("blocks",            "INTEGER"),
    _sf("turnovers",         "INTEGER"),
    _sf("pts_reb",           "INTEGER"),
    _sf("pts_ast",           "INTEGER"),
    _sf("reb_ast",           "INTEGER"),
    _sf("pra",               "INTEGER"),
    _sf("ingested_at",       "TIMESTAMP"),
])

print("All NBA tables created.")
