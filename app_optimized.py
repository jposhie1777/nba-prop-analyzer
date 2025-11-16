# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from google.oauth2 import service_account
from google.cloud import bigquery

# ------------------------------------------------------
# STREAMLIT CONFIG
# ------------------------------------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

PROJECT_ID = os.getenv("PROJECT_ID", "")
DATASET = "nba_prop_analyzer"
PROPS_TABLE = "todays_props_with_hit_rates"
HISTORICAL_TABLE = "historical_player_stats_for_trends"
GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT", "")

if not PROJECT_ID or not GCP_SERVICE_ACCOUNT:
    st.error("❌ Missing PROJECT_ID or GCP_SERVICE_ACCOUNT.")
    st.stop()

# ------------------------------------------------------
# GOOGLE CREDS
# ------------------------------------------------------
try:
    creds_dict = json.loads(GCP_SERVICE_ACCOUNT)
    base_credentials = service_account.Credentials.from_service_account_info(creds_dict)

    credentials = base_credentials.with_scopes([
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
    ])
    st.write("✅ Credentials loaded")
except Exception as e:
    st.error(f"❌ Credential Error: {e}")
    st.stop()

# ------------------------------------------------------
# BIGQUERY CLIENT
# ------------------------------------------------------
try:
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    bq_client.query("SELECT 1").result()
    st.sidebar.success("✅ BigQuery Connected")
except Exception as e:
    st.sidebar.error(f"❌ BigQuery Connection Error: {e}")
    st.stop()

# ------------------------------------------------------
# SQL QUERIES
# ------------------------------------------------------
PROPS_SQL = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{PROPS_TABLE}`"

HISTORICAL_SQL = f"""
SELECT player, player_team, home_team, visitor_team,
       game_date, opponent_team, home_away,
       pts, reb, ast, pra
FROM `{PROJECT_ID}.{DATASET}.{HISTORICAL_TABLE}`
ORDER BY game_date
"""

# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------
def format_moneyline(v):
    try:
        v = int(round(float(v)))
        return f"+{v}" if v > 0 else str(v)
    except:
        return "—"

def detect_stat(market):
    m = (market or "").lower()
    if "p+r+a" in m or "pra" in m: return "pra"
    if "assist" in m or "ast" in m: return "ast"
    if "reb" in m: return "reb"
    if "pt" in m or "point" in m: return "pts"
    return ""

def get_dynamic_averages(df):
    df = df.copy()
    def pick(row, h):
        stat = detect_stat(row["market"])
        return row.get(f"{stat}_last{h}", np.nan)
    df["L5 Avg"] = df.apply(lambda r: pick(r, 5), axis=1)
    df["L10 Avg"] = df.apply(lambda r: pick(r, 10), axis=1)
    df["L20 Avg"] = df.apply(lambda r: pick(r, 20), axis=1)
    return df

# ------------------------------------------------------
# LOAD DATA (CACHED)
# ------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_props():
    df = bq_client.query(PROPS_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df

@st.cache_data(show_spinner=True)
def load_hist():
    df = bq_client.query(HISTORICAL_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df

props_df = load_props()
hist_df = load_hist()

# ------------------------------------------------------
# FIX: MAKE ALL NUMERIC COLUMNS TRUE NUMBERS
# ------------------------------------------------------
numeric_cols = [
    "price", "line",
    "hit_rate_last5", "hit_rate_last10", "hit_rate_last20",
    "pts_last5", "pts_last10", "pts_last20",
    "reb_last5", "reb_last10", "reb_last20",
    "ast_last5", "ast_last10", "ast_last20",
    "pra_last5", "pra_last10", "pra_last20",
    "season_avg"
]

for col in numeric_cols:
    if col in props_df.columns:
        props_df[col] = pd.to_numeric(props_df[col], errors="coerce")

for col in ["pts", "reb", "ast", "pra"]:
    if col in hist_df.columns:
        hist_df[col] = pd.to_numeric(hist_df[col], errors="coerce")

# ------------------------------------------------------
# SAVED BETS
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

# ------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙ Filters")

games = ["All games"] + sorted((props_df["home_team"] + " vs " + props_df["visitor_team"]).unique())
sel_game = st.sidebar.selectbox("Game", games)

players = ["All players"] + sorted(props_df["player"].unique())
sel_player = st.sidebar.selectbox("Player", players)

markets = ["All Stats"] + sorted(props_df["market"].unique())
sel_market = st.sidebar.selectbox("Market", markets)

books = sorted(props_df["bookmaker"].unique())
default_books = [b for b in books if b.lower() in ("draftkings", "fanduel")] or books
sel_books = st.sidebar.multiselect("Bookmakers", books, default_books)

min_odds = int(props_df["price"].min())
max_odds = int(props_df["price"].max())
sel_odds = st.sidebar.slider("Odds Range", min_odds, max_odds, (min_odds, max_odds))

sel_hit10 = st.sidebar.slider("Min Hit Rate L10", 0.0, 1.0, 0.5)

# ------------------------------------------------------
# FILTER PROPS
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
# TAB 1 — PROPS OVERVIEW
# ------------------------------------------------------
with tab1:
    st.subheader("Props Overview")

    d = filter_props(props_df)

    if d.empty:
        st.info("No props match your filters.")
    else:
        d = get_dynamic_averages(d)
        d = d.sort_values("hit_rate_last10", ascending=False)

        d["Price"] = d["price"].apply(format_moneyline)
        d["Hit L5"] = d["hit_rate_last5"]
        d["Hit L10"] = d["hit_rate_last10"]
        d["Hit L20"] = d["hit_rate_last20"]

        cols = [
            "player", "market", "line", "Price", "bookmaker",
            "Hit L5", "Hit L10", "Hit L20",
            "L5 Avg", "L10 Avg", "L20 Avg"
        ]

        st.dataframe(d[cols], use_container_width=True)

# ------------------------------------------------------
# TAB 2 — TREND ANALYSIS
# ------------------------------------------------------
with tab2:
    st.subheader("Trend Analysis")

    players_list = ["(select)"] + sorted(props_df["player"].unique())
    p = st.selectbox("Player", players_list)

    if p != "(select)":
        mlist = sorted(props_df[props_df["player"] == p]["market"].unique())
        m = st.selectbox("Market", mlist)

        line_values = sorted(props_df[(props_df["player"] == p) & (props_df["market"] == m)]["line"].unique())
        line_pick = st.selectbox("Select Line", line_values)

        stat = detect_stat(m)

        # Only include played games
        df_hist = (
            hist_df[(hist_df["player"] == p) &
                   ((hist_df["pts"].notna()) |
                    (hist_df["reb"].notna()) |
                    (hist_df["ast"].notna()) |
                    (hist_df["pra"].notna()))]
            .sort_values("game_date")
            .tail(20)
        )

        if df_hist.empty:
            st.info("No recent games found.")
        else:
            # Season average
            season_row = props_df[(props_df["player"] == p) & (props_df["market"] == m)]
            if not season_row.empty:
                season_avg = float(season_row.iloc[0]["season_avg"])
            else:
                season_avg = df_hist[stat].mean()

            values = df_hist[stat].astype(float)
            colors = ["#21c36b" if v >= line_pick else "#e45757" for v in values]

            fig = go.Figure()

            fig.add_bar(
                x=df_hist["game_date"].dt.strftime("%Y-%m-%d"),
                y=values,
                marker_color=colors,
                name=stat.upper(),
            )

            fig.add_hline(y=line_pick, line_dash="dash", line_color="red",
                          name=f"Line {line_pick}")

            fig.add_hline(y=season_avg, line_dash="dot", line_color="blue",
                          name=f"Season Avg ({season_avg:.1f})")

            fig.update_layout(
                height=450,
                xaxis=dict(type="category"),  # removes offseason gaps
                xaxis_title="Game Date",
                yaxis_title=stat.upper(),
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="center",
                    x=0.5
                )
            )

            st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------
# TAB 3 — SAVED BETS
# ------------------------------------------------------
with tab3:
    st.subheader("Saved Bets")

    if not st.session_state.saved_bets:
        st.info("No saved bets.")
    else:
        df_save = pd.DataFrame(st.session_state.saved_bets)
        st.dataframe(df_save, use_container_width=True)

        csv = df_save.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv, "saved_bets.csv", "text/csv")

# ------------------------------------------------------
# END
# ------------------------------------------------------
