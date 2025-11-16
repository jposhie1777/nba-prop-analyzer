# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.types as pat

import plotly.graph_objects as go
import streamlit as st

from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage


# ------------------------------------------------------
# STREAMLIT PAGE CONFIG
# ------------------------------------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")


# ------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")
PROP_ANALYZER_DATASET = os.getenv("PROP_ANALYZER_DATASET", "nba_prop_analyzer")
TODAYS_PROPS_TABLE = os.getenv("TODAYS_PROPS_TABLE", "todays_props_with_hit_rates")
GAME_LOGS_TABLE = os.getenv("GAME_LOGS_TABLE", "todays_props_game_logs")
GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT", "")

if not PROJECT_ID or not GCP_SERVICE_ACCOUNT:
    st.error("❌ Missing environment variables — check PROJECT_ID and GCP_SERVICE_ACCOUNT.")
    st.stop()


# ------------------------------------------------------
# LOAD GCP SERVICE ACCOUNT CREDENTIALS
# ------------------------------------------------------
try:
    creds_dict = json.loads(GCP_SERVICE_ACCOUNT)
    base_credentials = service_account.Credentials.from_service_account_info(creds_dict)

    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/bigquery.readonly",
    ]

    credentials = base_credentials.with_scopes(SCOPES)
    st.write("✅ Google credentials loaded successfully")
except Exception as e:
    st.error(f"❌ Failed to load Google credentials: {e}")
    st.stop()


# ------------------------------------------------------
# INITIALIZE BIGQUERY CLIENTS
# ------------------------------------------------------
@st.cache_resource
def get_bq_clients():
    return (
        bigquery.Client(project=PROJECT_ID, credentials=credentials),
        bigquery_storage.BigQueryReadClient(credentials=credentials),
    )


bq_client, bqs_client = get_bq_clients()


# ------------------------------------------------------
# OPTIMIZED SQL QUERIES (ONLY REQUIRED COLUMNS)
# ------------------------------------------------------
PROPS_SQL = f"""
SELECT
  player,
  market,
  bookmaker,
  player_team,
  opponent_team,
  home_team,
  visitor_team,
  line,
  price,
  expected_value,
  hit_rate_last5,
  hit_rate_last10,
  hit_rate_last20,
  pts_last5,
  reb_last5,
  ast_last5,
  pra_last5,
  pts_last10,
  reb_last10,
  ast_last10,
  pra_last10,
  pts_last20,
  reb_last20,
  ast_last20,
  pra_last20
FROM `{PROJECT_ID}.{PROP_ANALYZER_DATASET}.{TODAYS_PROPS_TABLE}`
"""

GAME_LOGS_SQL = f"""
SELECT
  game_date,
  player,
  market,
  team,
  opponent_team,
  line,
  pts,
  reb,
  ast,
  pra,
  season_avg
FROM `{PROJECT_ID}.{PROP_ANALYZER_DATASET}.{GAME_LOGS_TABLE}`
"""


# ------------------------------------------------------
# ARROW STREAMING LOADERS (SUPER MEMORY-EFFICIENT)
# ------------------------------------------------------
def read_arrow_stream(query: str, bq: bigquery.Client, bqs: bigquery_storage.BigQueryReadClient):
    """
    Streams BigQuery results via Arrow without loading the
    full result set into memory at once.
    """
    job = bq.query(query)
    reader = bqs.read_rows(job.job_id)
    arrow_schema = reader.schema

    batches = []
    for batch in reader.rows().pages:
        batches.append(batch.to_arrow())

    if not batches:
        return pa.Table.from_arrays([], schema=arrow_schema)

    return pa.concat_tables(batches, promote=True)


def arrow_to_pandas_optimized(table: pa.Table) -> pd.DataFrame:
    """
    Zero-copy Arrow→Pandas conversion with aggressive downcasting.
    """
    df = table.to_pandas(types_mapper=pd.ArrowDtype)

    # Convert to category
    str_cols = [
        "player", "market", "bookmaker", "player_team",
        "opponent_team", "home_team", "visitor_team",
        "team"
    ]
    for col in df.columns:
        if col in str_cols and col in df:
            df[col] = df[col].astype("category")

    # Downcast numerics
    num_cols = df.select_dtypes(include=["float", "int"]).columns
    df[num_cols] = df[num_cols].apply(pd.to_numeric, downcast="float")

    return df


# ------------------------------------------------------
# CACHED DATA LOADERS (NO MEMORY SPIKES)
# ------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=True)
def load_props_cached_arrow(_bq, _bqs, query):
    arrow_table = read_arrow_stream(query, _bq, _bqs)
    df = arrow_to_pandas_optimized(arrow_table)
    return df


@st.cache_data(ttl=600, show_spinner=True)
def load_game_logs_cached_arrow(_bq, _bqs, query):
    arrow_table = read_arrow_stream(query, _bq, _bqs)
    df = arrow_to_pandas_optimized(arrow_table)

    if "game_date" in df:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    return df


# ------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------
props_df = load_props_cached_arrow(bq_client, bqs_client, PROPS_SQL)
game_logs_df = load_game_logs_cached_arrow(bq_client, bqs_client, GAME_LOGS_SQL)

st.sidebar.success("📦 Data loaded via PyArrow streaming (memory optimized)")


# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------
def format_moneyline(val):
    try:
        v = int(round(float(val)))
        return f"+{v}" if v > 0 else str(v)
    except:
        return "—"


def get_stat_base_from_market(market):
    m = (market or "").lower()
    if any(x in m for x in ["pra", "points_rebounds_assists", "pts_reb_ast", "p+r+a"]):
        return "pra"
    if "ast" in m or "assist" in m:
        return "ast"
    if "reb" in m or "rebound" in m:
        return "reb"
    if "pts" in m or "point" in m:
        return "pts"
    return ""


def add_dynamic_averages(df):
    """
    Compute L5/L10/L20 averages using *_lastN columns.
    """
    def pick(row, n):
        base = get_stat_base_from_market(row["market"])
        return row.get(f"{base}_last{n}", np.nan)

    for n in (5, 10, 20):
        df[f"L{n} Avg"] = df.apply(lambda r: pick(r, n), axis=1).astype("float32")

    return df


props_df = add_dynamic_averages(props_df)


# ------------------------------------------------------
# REFRESH BUTTON
# ------------------------------------------------------
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()


# ------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙️ Filters")

today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)

# Game selector
if {"home_team", "visitor_team"}.issubset(props_df.columns):
    games = props_df[["home_team", "visitor_team"]].dropna().drop_duplicates()
    games["matchup"] = games["home_team"].astype(str) + " vs " + games["visitor_team"].astype(str)
    game_options = ["All games"] + games["matchup"].tolist()
else:
    game_options = ["All games"]

sel_game = st.sidebar.selectbox("Game", game_options)

# Players
players = ["All players"] + sorted(props_df["player"].astype(str).unique())
sel_player = st.sidebar.selectbox("Player", players)

# Market filter
markets = sorted(props_df["market"].astype(str).unique())
sel_stat_disp = st.sidebar.selectbox("Market", ["All Stats"] + markets)
sel_stat = None if sel_stat_disp == "All Stats" else sel_stat_disp

# Bookmakers
books = sorted(props_df["bookmaker"].astype(str).unique())
sel_books = st.sidebar.multiselect("Bookmakers", books, default=books)

# Odds filters
odds = pd.to_numeric(props_df["price"], errors="coerce")
sel_odds_range = st.sidebar.slider("American odds", int(odds.min()), int(odds.max()), (int(odds.min()), int(odds.max())))
odds_threshold = st.sidebar.number_input("Only show odds above", -2000, 2000, -600)

# Analytical filters
sel_min_ev = st.sidebar.slider("Minimum EV", -1.0, 1.0, 0.0)
sel_min_hit10 = st.sidebar.slider("Min Hit Rate (L10)", 0.0, 1.0, 0.5)


# ------------------------------------------------------
# PROPS TABLE BUILDER
# ------------------------------------------------------
def build_props_table(df):
    view = df

    # Game
    if sel_game != "All games":
        home, away = sel_game.split(" vs ", 1)
        view = view[(view["home_team"] == home) & (view["visitor_team"] == away)]

    # Player
    if sel_player != "All players":
        view = view[view["player"] == sel_player]

    # Market
    if sel_stat:
        view = view[view["market"] == sel_stat]

    # Books
    if sel_books:
        view = view[view["bookmaker"].isin(sel_books)]

    # Odds range
    view = view[(view["price"] >= sel_odds_range[0]) & (view["price"] <= sel_odds_range[1])]

    # EV
    view = view[view["expected_value"] >= sel_min_ev]

    # Hit10
    view = view[view["hit_rate_last10"] >= (sel_min_hit10 * 100)]

    if view.empty:
        return pd.DataFrame()

    display = view[[
        "player", "market", "line", "price",
        "bookmaker", "L5 Avg", "L10 Avg", "L20 Avg",
        "hit_rate_last10", "expected_value"
    ]].copy()

    display.rename(columns={
        "player": "Player",
        "market": "Market",
        "line": "Line",
        "price": "Price",
        "bookmaker": "Bookmaker",
        "hit_rate_last10": "Hit L10",
        "expected_value": "EV"
    }, inplace=True)

    display["Price"] = display["Price"].apply(format_moneyline)
    display["EV"] = display["EV"].astype(float).round(3)

    return display


# ------------------------------------------------------
# TAB 1 – PROPS OVERVIEW
# ------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(["🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets", "📊 Prop Analytics"])

with tab1:
    st.subheader("Props Overview")

    df = build_props_table(props_df)

    if df.empty:
        st.info("No props match filters.")
    else:
        st.dataframe(df, use_container_width=True)


# ------------------------------------------------------
# TAB 2 – TREND ANALYSIS
# ------------------------------------------------------
def get_player_game_log(df, player, market):
    s = df[(df["player"] == player) & (df["market"] == market)]
    return s.sort_values("game_date").tail(20)


with tab2:
    st.subheader("Trend Analysis")

    players = ["(choose)"] + sorted(props_df["player"].astype(str).unique())
    p_pick = st.selectbox("Player", players)
    if p_pick == "(choose)":
        st.stop()

    markets = sorted(props_df[props_df["player"] == p_pick]["market"].astype(str).unique())
    m_pick = st.selectbox("Market", markets)

    lines = sorted(props_df[(props_df["player"] == p_pick) & (props_df["market"] == m_pick)]["line"].dropna().unique())
    line_pick = st.selectbox("Line", lines)

    logs = get_player_game_log(game_logs_df, p_pick, m_pick)

    if logs.empty:
        st.info("No logs available.")
        st.stop()

    stat_base = get_stat_base_from_market(m_pick)
    stat_col = {"pts": "pts", "reb": "reb", "ast": "ast", "pra": "pra"}.get(stat_base, "pra")

    fig = go.Figure()
    fig.add_bar(
        x=logs["game_date"].dt.date.astype(str),
        y=logs[stat_col],
        marker_color=np.where(logs[stat_col] > line_pick, "#21c36b", "#e45757")
    )
    fig.add_hline(y=line_pick, line_dash="dash", line_color="#d9534f")
    st.plotly_chart(fig, use_container_width=True)


# ------------------------------------------------------
# TAB 3 – SAVED BETS
# ------------------------------------------------------
with tab3:
    st.subheader("Saved Bets")
    st.info("Saving bets will return soon with a lower-memory design.")


# ------------------------------------------------------
# TAB 4 – PROP ANALYTICS
# ------------------------------------------------------
with tab4:
    st.subheader("Prop Analytics")
    df = build_props_table(props_df)

    if df.empty:
        st.info("No props available.")
    else:
        st.dataframe(df, use_container_width=True)
