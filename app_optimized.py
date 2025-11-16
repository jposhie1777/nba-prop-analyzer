# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from google.oauth2 import service_account
from google.cloud import bigquery

# ------------------------------------------------------
# STREAMLIT PAGE CONFIG
# ------------------------------------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# ------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")
DATASET = "nba_prop_analyzer"
PROPS_TABLE = "todays_props_with_hit_rates"
HISTORICAL_TABLE = "historical_player_stats_for_trends"

GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT", "")

if not PROJECT_ID or not GCP_SERVICE_ACCOUNT:
    st.error("❌ Missing PROJECT_ID or GCP_SERVICE_ACCOUNT.")
    st.stop()

# ------------------------------------------------------
# LOAD GOOGLE CREDENTIALS
# ------------------------------------------------------
try:
    creds_dict = json.loads(GCP_SERVICE_ACCOUNT)
    base_credentials = service_account.Credentials.from_service_account_info(creds_dict)

    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
    ]

    credentials = base_credentials.with_scopes(SCOPES)
    st.write("✅ Credentials loaded successfully!")

except Exception as e:
    st.error(f"❌ Failed to load Google credentials: {e}")
    st.stop()

# ------------------------------------------------------
# BIGQUERY CLIENT
# ------------------------------------------------------
try:
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    _ = bq_client.query("SELECT 1").result()
    st.sidebar.success("✅ Connected to BigQuery")
except Exception as e:
    st.sidebar.error(f"❌ BigQuery connection failed: {e}")
    st.stop()

# ------------------------------------------------------
# SQL QUERIES
# ------------------------------------------------------
PROPS_SQL = f"""
SELECT *
FROM `{PROJECT_ID}.{DATASET}.{PROPS_TABLE}`
"""

HISTORICAL_SQL = f"""
SELECT
  player,
  player_team,
  home_team,
  visitor_team,
  game_date,
  opponent_team,
  home_away,
  pts,
  reb,
  ast,
  pra
FROM `{PROJECT_ID}.{DATASET}.{HISTORICAL_TABLE}`
ORDER BY game_date
"""

# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------
def format_moneyline(value):
    try:
        v = int(round(float(value)))
        return f"+{v}" if v > 0 else str(v)
    except:
        return "—"


def detect_stat(market: str) -> str:
    """Identify pts/reb/ast/pra from market string."""
    m = (market or "").lower()

    if "p+r+a" in m or "pra" in m:
        return "pra"
    if "ast" in m:
        return "ast"
    if "reb" in m:
        return "reb"
    if "pt" in m or "point" in m:
        return "pts"
    return ""


# ------------------------------------------------------
# LOAD DATA (CACHED)
# ------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_props():
    df = bq_client.query(PROPS_SQL).to_dataframe()
    df.columns = [c.strip() for c in df.columns]
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df


@st.cache_data(show_spinner=True)
def load_historical():
    df = bq_client.query(HISTORICAL_SQL).to_dataframe()
    df.columns = [c.strip() for c in df.columns]
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df


props_df = load_props()
historical_df = load_historical()

# ------------------------------------------------------
# SAVED BETS
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []


# ------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙ Filters")

# Game filter
games = sorted(
    (props_df["home_team"] + " vs " + props_df["visitor_team"])
    .dropna()
    .unique()
    .tolist()
)
games = ["All games"] + games
sel_game = st.sidebar.selectbox("Game", games)

# Player filter
players = sorted(props_df["player"].dropna().unique())
players = ["All players"] + players
sel_player = st.sidebar.selectbox("Player", players)

# Market filter
markets = sorted(props_df["market"].dropna().unique())
markets = ["All Stats"] + markets
sel_market = st.sidebar.selectbox("Market", markets)

# Bookmaker filter (default DK + FD)
books = sorted(props_df["bookmaker"].dropna().unique())

default_books = [b for b in books if b.lower() in ("draftkings", "fanduel")]
if not default_books:
    default_books = books  # fallback

sel_books = st.sidebar.multiselect(
    "Bookmaker",
    books,
    default=default_books,
)

# Odds slider
min_odds = int(props_df["price"].min())
max_odds = int(props_df["price"].max())
sel_odds = st.sidebar.slider("Odds Range", min_odds, max_odds, (min_odds, max_odds))

# Hit rate filter
sel_hit10 = st.sidebar.slider("Min Hit Rate L10", 0.0, 1.0, 0.5, step=0.01)

# ------------------------------------------------------
# FILTER PROPS DATA
# ------------------------------------------------------
def filter_props(df):
    d = df.copy()

    if sel_game != "All games":
        home, away = sel_game.split(" vs ")
        d = d[(d["home_team"] == home) & (d["visitor_team"] == away)]

    if sel_player != "All players":
        d = d[d["player"] == sel_player]

    if sel_market != "All Stats":
        d = d[d["market"] == sel_market]

    d = d[d["bookmaker"].isin(sel_books)]
    d = d[d["price"].between(sel_odds[0], sel_odds[1])]
    d = d[d["hit_rate_last10"] >= sel_hit10]

    return d


# ------------------------------------------------------
# TABS
# ------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets"])

# ------------------------------------------------------
# TAB 1 – PROPS OVERVIEW
# ------------------------------------------------------
with tab1:
    st.subheader("Props Overview")

    d = filter_props(props_df)

    if d.empty:
        st.info("No props match your filters.")
    else:
        # Sort by Hit Rate 10 (descending)
        d = d.sort_values("hit_rate_last10", ascending=False, na_position="last")

        d["Price"] = d["price"].apply(format_moneyline)
        d["Hit L5"] = d["hit_rate_last5"]
        d["Hit L10"] = d["hit_rate_last10"]
        d["Hit L20"] = d["hit_rate_last20"]

        display_cols = [
            "player", "market", "line", "Price", "bookmaker",
            "Hit L5", "Hit L10", "Hit L20",
            "pts_last5", "pts_last10", "pts_last20"
        ]

        st.dataframe(d[display_cols], use_container_width=True)

# ------------------------------------------------------
# TAB 2 – TREND ANALYSIS
# ------------------------------------------------------
with tab2:
    st.subheader("Trend Analysis")

    players = ["(select)"] + sorted(props_df["player"].unique().tolist())
    p = st.selectbox("Player", players)

    if p != "(select)":
        markets = sorted(props_df[props_df["player"] == p]["market"].unique())
        m = st.selectbox("Market", markets)

        line_values = sorted(
            props_df[(props_df["player"] == p) & (props_df["market"] == m)]["line"]
            .dropna()
            .unique()
            .tolist()
        )
        line_pick = st.selectbox("Select Line", line_values)

        st.markdown("### 📊 Game Log Trend")
        stat = detect_stat(m)

        df_hist = historical_df[historical_df["player"] == p].sort_values("game_date").tail(20)

        fig = go.Figure()
        fig.add_bar(
            x=df_hist["game_date"].dt.strftime("%Y-%m-%d"),
            y=df_hist[stat],
            name=stat.upper()
        )

        fig.add_hline(y=line_pick, line_dash="dash", line_color="red", name="Line")

        fig.update_layout(height=450, xaxis_title="Game Date", yaxis_title=stat.upper())
        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------
# TAB 3 – SAVED BETS
# ------------------------------------------------------
with tab3:
    st.subheader("Saved Bets")

    if not st.session_state.saved_bets:
        st.info("No saved bets yet.")
    else:
        df_save = pd.DataFrame(st.session_state.saved_bets)
        st.dataframe(df_save, use_container_width=True)

        csv = df_save.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv, "saved_bets.csv", "text/csv")

# ------------------------------------------------------
# END OF SCRIPT
# ------------------------------------------------------
