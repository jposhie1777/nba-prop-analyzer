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
# HELPER FUNCTIONS
# ------------------------------------------------------
def format_moneyline(value):
    try:
        v = int(round(float(value)))
        return f"+{v}" if v > 0 else str(v)
    except:
        return "—"

def detect_stat(market):
    m = (market or "").lower()

    if "p+r+a" in m or "pra" in m:
        return "pra"
    if "assist" in m or "ast" in m:
        return "ast"
    if "reb" in m:
        return "reb"
    if "pt" in m or "point" in m:
        return "pts"
    return ""

def get_dynamic_averages(df):
    df = df.copy()
    def pick(row, horizon):
        stat = detect_stat(row["market"])
        col = f"{stat}_last{horizon}"
        return row.get(col, np.nan)

    df["L5 Avg"] = df.apply(lambda r: pick(r, 5), axis=1)
    df["L10 Avg"] = df.apply(lambda r: pick(r, 10), axis=1)
    df["L20 Avg"] = df.apply(lambda r: pick(r, 20), axis=1)
    return df

# ---------------------------------------------------------
# DEFENSIVE MATCHUP COLUMNS
# ---------------------------------------------------------
def add_defensive_matchups(df):
    df = df.copy()
    stat = df["market"].apply(detect_stat)

    pos_map = {
        "pts": "opp_pos_pts_rank",
        "reb": "opp_pos_reb_rank",
        "ast": "opp_pos_ast_rank",
        "pra": "opp_pos_pra_rank",
    }

    overall_map = {
        "pts": "opp_overall_pts_rank",
        "reb": "opp_overall_reb_rank",
        "ast": "opp_overall_ast_rank",
        "pra": "opp_overall_pra_rank",
    }

    df["Pos Def Rank"] = [
        df.loc[i, pos_map.get(stat[i], None)]
        if pos_map.get(stat[i], None) in df.columns else np.nan
        for i in df.index
    ]

    df["Overall Def Rank"] = [
        df.loc[i, overall_map.get(stat[i], None)]
        if overall_map.get(stat[i], None) in df.columns else np.nan
        for i in df.index
    ]

    df["Matchup Difficulty"] = df.get("matchup_difficulty_score", np.nan)
    return df

# ---------------------------------------------------------
# DEFENSE COLOR CODING
# ---------------------------------------------------------
def apply_defense_color(val):
    if pd.isna(val):
        return "background-color: #444444; color: white;"
    try:
        v = int(val)
    except:
        return ""

    if v <= 5:
        return "background-color: #d9534f; color: white;"   # red
    elif v <= 15:
        return "background-color: #f0ad4e; color: black;"   # orange
    elif v <= 25:
        return "background-color: #ffd500; color: black;"   # yellow
    else:
        return "background-color: #5cb85c; color: white;"   # green

# ------------------------------------------------------
# LOAD DATA
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
# SIDEBAR + SESSION
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

st.sidebar.header("⚙ Filters")

games = ["All games"] + sorted(
    (props_df["home_team"] + " vs " + props_df["visitor_team"]).dropna().unique()
)
sel_game = st.sidebar.selectbox("Game", games)

players = ["All players"] + sorted(props_df["player"].dropna().unique())
sel_player = st.sidebar.selectbox("Player", players)

markets = ["All Stats"] + sorted(props_df["market"].dropna().unique())
sel_market = st.sidebar.selectbox("Market", markets)

books = sorted(props_df["bookmaker"].dropna().unique())
default_books = [b for b in books if b.lower() in ("draftkings", "fanduel")] or books
sel_books = st.sidebar.multiselect("Bookmaker", books, default=default_books)

min_odds = int(props_df["price"].min())
max_odds = int(props_df["price"].max())
sel_odds = st.sidebar.slider("Odds Range", min_odds, max_odds, (min_odds, max_odds))

sel_hit10 = st.sidebar.slider("Min Hit Rate L10", 0.0, 1.0, 0.5)

# ------------------------------------------------------
# FILTER FUNCTION
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
        d = get_dynamic_averages(d)
        d = add_defensive_matchups(d)
        d = d.sort_values("hit_rate_last10", ascending=False)

        # Display columns
        display_cols = [
            "player", "market", "line", "Price", "bookmaker",
            "Pos Def Rank", "Overall Def Rank", "Matchup Difficulty",
            "hit_rate_last5", "hit_rate_last10", "hit_rate_last20",
            "L5 Avg", "L10 Avg", "L20 Avg"
        ]

        d["Price"] = d["price"].apply(format_moneyline)

        styled = d[display_cols].style.applymap(
            apply_defense_color,
            subset=["Pos Def Rank", "Overall Def Rank", "Matchup Difficulty"]
        )

        st.dataframe(styled, use_container_width=True)

# ------------------------------------------------------
# TAB 2 – TREND ANALYSIS (FULLY UPDATED)
# ------------------------------------------------------
with tab2:
    st.subheader("Trend Analysis")

    players_list = ["(select)"] + sorted(props_df["player"].unique())
    p = st.selectbox("Player", players_list)

    if p != "(select)":
        markets = sorted(props_df[props_df["player"] == p]["market"].unique())
        m = st.selectbox("Market", markets)

        line_values = sorted(
            props_df[(props_df["player"] == p) & (props_df["market"] == m)]["line"]
            .dropna()
            .unique()
        )
        line_pick = st.selectbox("Select Line", line_values)

        stat = detect_stat(m)

        # Pull correct prop row for defense data
        prop_row = props_df[
            (props_df["player"] == p) &
            (props_df["market"] == m) &
            (props_df["line"] == line_pick)
        ].iloc[0]

        overall_def_rank = prop_row[f"opp_overall_{stat}_rank"]

        # Background shade based on defense
        if overall_def_rank <= 5:
            def_bg = "#662222"
        elif overall_def_rank <= 15:
            def_bg = "#664400"
        elif overall_def_rank <= 25:
            def_bg = "#665500"
        else:
            def_bg = "#335533"

        df_hist = (
            historical_df[(historical_df["player"] == p) & (historical_df[stat].notna())]
            .sort_values("game_date")
            .tail(20)
        )

        df_hist["color"] = np.where(df_hist[stat] > line_pick, "green", "red")

        hover_text = [
            f"Date: {d.strftime('%b %d')}<br>"
            f"{stat.upper()}: {v}<br>"
            f"Pos Def Rank: {prop_row[f'opp_pos_{stat}_rank']}<br>"
            f"Overall Def Rank: {overall_def_rank}<br>"
            f"Difficulty Score: {prop_row['matchup_difficulty_score']}"
            for d, v in zip(df_hist["game_date"], df_hist[stat])
        ]

        fig = go.Figure()

        fig.add_bar(
            x=df_hist["game_date"].dt.strftime("%b %d"),
            y=df_hist[stat],
            marker_color=df_hist["color"],
            hovertext=hover_text,
            hoverinfo="text",
            name=stat.upper()
        )

        fig.add_hline(
            y=line_pick,
            line_dash="dash",
            line_color="white",
            annotation_text=f"Line: {line_pick}",
            annotation_position="top left"
        )

        fig.update_layout(
            height=450,
            xaxis_title="Game Date",
            yaxis_title=stat.upper(),
            xaxis=dict(type="category"),
            plot_bgcolor=def_bg,
            paper_bgcolor="#222222",
            font=dict(color="white"),
        )

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

