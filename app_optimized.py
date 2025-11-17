# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import pytz
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery

# ------------------------------------------------------
# TIMEZONE (EST)
# ------------------------------------------------------
EST = pytz.timezone("America/New_York")

# ------------------------------------------------------
# STREAMLIT CONFIG
# ------------------------------------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# Global CSS to center table text
st.markdown(
    """
    <style>
    table td, table th {
        text-align: center !important;
        vertical-align: middle !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")
DATASET = "nba_prop_analyzer"
PROPS_TABLE = "todays_props_with_hit_rates"
HISTORICAL_TABLE = "historical_player_stats_for_trends"
SERVICE_JSON = os.getenv("GCP_SERVICE_ACCOUNT", "")

if not PROJECT_ID or not SERVICE_JSON:
    st.error("❌ Missing PROJECT_ID or GCP_SERVICE_ACCOUNT environment variables.")
    st.stop()

# ------------------------------------------------------
# SQL STATEMENTS
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
# AUTHENTICATION
# ------------------------------------------------------
try:
    creds_dict = json.loads(SERVICE_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery",
        ],
    )
    st.write("✅ Credentials loaded.")
except Exception as e:
    st.error(f"❌ Credential error: {e}")
    st.stop()

# ------------------------------------------------------
# BIGQUERY CLIENT
# ------------------------------------------------------
try:
    bq_client = bigquery.Client(credentials=creds, project=PROJECT_ID)
    _ = bq_client.query("SELECT 1").result()
    st.sidebar.success("Connected to BigQuery")
except Exception as e:
    st.sidebar.error(f"BigQuery connection failed: {e}")
    st.stop()

# ------------------------------------------------------
# LOGOS (STATIC)
# ------------------------------------------------------
TEAM_LOGOS = {
    "ATL": "https://a.espncdn.com/i/teamlogos/nba/500/atl.png",
    "BOS": "https://a.espncdn.com/i/teamlogos/nba/500/bos.png",
    "BKN": "https://a.espncdn.com/i/teamlogos/nba/500/bkn.png",
    "CHA": "https://a.espncdn.com/i/teamlogos/nba/500/cha.png",
    "CHI": "https://a.espncdn.com/i/teamlogos/nba/500/chi.png",
    "CLE": "https://a.espncdn.com/i/teamlogos/nba/500/cle.png",
    "DAL": "https://a.espncdn.com/i/teamlogos/nba/500/dal.png",
    "DEN": "https://a.espncdn.com/i/teamlogos/nba/500/den.png",
    "DET": "https://a.espncdn.com/i/teamlogos/nba/500/det.png",
    "GSW": "https://a.espncdn.com/i/teamlogos/nba/500/gs.png",
    "HOU": "https://a.espncdn.com/i/teamlogos/nba/500/hou.png",
    "IND": "https://a.espncdn.com/i/teamlogos/nba/500/ind.png",
    "LAC": "https://a.espncdn.com/i/teamlogos/nba/500/lac.png",
    "LAL": "https://a.espncdn.com/i/teamlogos/nba/500/lal.png",
    "MEM": "https://a.espncdn.com/i/teamlogos/nba/500/mem.png",
    "MIA": "https://a.espncdn.com/i/teamlogos/nba/500/mia.png",
    "MIL": "https://a.espncdn.com/i/teamlogos/nba/500/mil.png",
    "MIN": "https://a.espncdn.com/i/teamlogos/nba/500/min.png",
    "NOP": "https://a.espncdn.com/i/teamlogos/nba/500/no.png",
    "NYK": "https://a.espncdn.com/i/teamlogos/nba/500/ny.png",
    "OKC": "https://a.espncdn.com/i/teamlogos/nba/500/okc.png",
    "ORL": "https://a.espncdn.com/i/teamlogos/nba/500/orl.png",
    "PHI": "https://a.espncdn.com/i/teamlogos/nba/500/phi.png",
    "PHX": "https://a.espncdn.com/i/teamlogos/nba/500/phx.png",
    "POR": "https://a.espncdn.com/i/teamlogos/nba/500/por.png",
    "SAC": "https://a.espncdn.com/i/teamlogos/nba/500/sac.png",
    "SAS": "https://a.espncdn.com/i/teamlogos/nba/500/sa.png",
    "TOR": "https://a.espncdn.com/i/teamlogos/nba/500/tor.png",
    "UTA": "https://a.espncdn.com/i/teamlogos/nba/500/utah.png",
    "WAS": "https://a.espncdn.com/i/teamlogos/nba/500/wsh.png",
}

# ------------------------------------------------------
# UTILITY FUNCTIONS
# ------------------------------------------------------
def format_moneyline(v):
    try:
        v = float(v)
        v = int(round(v))
        return f"+{v}" if v > 0 else str(v)
    except Exception:
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

    def pull(row, n):
        stat = detect_stat(row["market"])
        col = f"{stat}_last{n}"
        return row.get(col, np.nan)

    df["L5 Avg"] = df.apply(lambda r: pull(r, 5), axis=1)
    df["L10 Avg"] = df.apply(lambda r: pull(r, 10), axis=1)
    df["L20 Avg"] = df.apply(lambda r: pull(r, 20), axis=1)
    return df

def add_defense(df):
    df = df.copy()
    stat = df["market"].apply(detect_stat)

    pos = {
        "pts": "opp_pos_pts_rank",
        "reb": "opp_pos_reb_rank",
        "ast": "opp_pos_ast_rank",
        "pra": "opp_pos_pra_rank",
    }
    overall = {
        "pts": "opp_overall_pts_rank",
        "reb": "opp_overall_reb_rank",
        "ast": "opp_overall_ast_rank",
        "pra": "opp_overall_pra_rank",
    }

    df["Pos Def Rank"] = [
        df.loc[i, pos.get(stat[i])] if pos.get(stat[i]) in df.columns else ""
        for i in df.index
    ]
    df["Overall Def Rank"] = [
        df.loc[i, overall.get(stat[i])] if overall.get(stat[i]) in df.columns else ""
        for i in df.index
    ]
    df["Matchup Difficulty"] = df.get("matchup_difficulty_score", np.nan)
    return df

def format_display(df):
    df = df.copy()

    df["Matchup Difficulty"] = df["Matchup Difficulty"].apply(
        lambda x: f"{int(round(x))}" if pd.notna(x) else ""
    )

    for col in ["hit_rate_last5", "hit_rate_last10", "hit_rate_last20"]:
        def fmt(x):
            if pd.isna(x):
                return ""
            if 0 <= x <= 1:
                return f"{int(round(x * 100))}%"
            return f"{int(round(x))}%"

        df[col] = df[col].apply(fmt)

    for col in ["L5 Avg", "L10 Avg", "L20 Avg"]:
        df[col] = df[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")

    return df

# ------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------
@st.cache_data
def load_props():
    df = bq_client.query(PROPS_SQL).to_dataframe()
    df.columns = df.columns.str.strip()

    df["game_date"] = (
        pd.to_datetime(df["game_date"], errors="coerce")
        .dt.tz_localize("UTC")
        .dt.tz_convert(EST)
    )

    df["home_team"] = df["home_team"].fillna("").astype(str)
    df["visitor_team"] = df["visitor_team"].fillna("").astype(str)
    df["opponent_team"] = df["opponent_team"].fillna("").astype(str)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["hit_rate_last10"] = pd.to_numeric(df["hit_rate_last10"], errors="coerce")

    return df

@st.cache_data
def load_historical():
    df = bq_client.query(HISTORICAL_SQL).to_dataframe()
    df.columns = df.columns.str.strip()

    df["game_date"] = (
        pd.to_datetime(df["game_date"], errors="coerce")
        .dt.tz_localize("UTC")
        .dt.tz_convert(EST)
    )

    df["opponent_team"] = df["opponent_team"].fillna("").astype(str)
    return df

props_df = load_props()
historical_df = load_historical()

# ------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

st.sidebar.header("Filters")

games_list = (props_df["home_team"] + " vs " + props_df["visitor_team"]).astype(str)
games = ["All games"] + sorted(games_list.unique())
sel_game = st.sidebar.selectbox("Game", games)

players = ["All players"] + sorted(props_df["player"].fillna("").astype(str).unique())
sel_player = st.sidebar.selectbox("Player", players)

markets = ["All Stats"] + sorted(props_df["market"].fillna("").astype(str).unique())
sel_market = st.sidebar.selectbox("Market", markets)

books = sorted(props_df["bookmaker"].fillna("").astype(str).unique())
default_books = [b for b in books if b.lower() in ("draftkings", "fanduel")] or books
sel_books = st.sidebar.multiselect("Bookmaker", books, default=default_books)

od_min = int(props_df["price"].min())
od_max = int(props_df["price"].max())
sel_odds = st.sidebar.slider("Odds Range", od_min, od_max, (od_min, od_max))

sel_hit10 = st.sidebar.slider("Min Hit Rate L10", 0.0, 1.0, 0.5)

# ------------------------------------------------------
# FILTER FUNCTION
# ------------------------------------------------------
def filter_props(df):
    d = df.copy()

    d["price"] = pd.to_numeric(d["price"], errors="coerce")
    d["hit_rate_last10"] = pd.to_numeric(d["hit_rate_last10"], errors="coerce")

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
tab1, tab2, tab3, tab4 = st.tabs(
    ["🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets", "📊 Prop Analytics"]
)

# ------------------------------------------------------
# TAB 1 — PROPS OVERVIEW (sortable dataframe)
# ------------------------------------------------------
with tab1:
    st.subheader("Props Overview")

    d = filter_props(props_df)

    if d.empty:
        st.info("No props match your filters.")
    else:
        d = get_dynamic_averages(d)
        d = add_defense(d)
        d["Price"] = d["price"].apply(format_moneyline)
        d = format_display(d)

        # Default sort (same behavior)
        d = d.sort_values("hit_rate_last10", ascending=False)

        cols = [
            "player",
            "market",
            "line",
            "Price",
            "bookmaker",
            "Pos Def Rank",
            "Overall Def Rank",
            "Matchup Difficulty",
            "hit_rate_last5",
            "hit_rate_last10",
            "hit_rate_last20",
            "L5 Avg",
            "L10 Avg",
            "L20 Avg",
        ]

        st.dataframe(
            d[cols],
            use_container_width=True,
            hide_index=True
        )

# ------------------------------------------------------
# TAB 2 — TREND ANALYSIS
# ------------------------------------------------------
with tab2:
    st.subheader("Trend Analysis")

    p = st.selectbox("Player", ["(select)"] + sorted(props_df["player"].unique()))
    if p != "(select)":
        markets = sorted(props_df[props_df["player"] == p]["market"].unique())
        m = st.selectbox("Market", markets)

        lines = sorted(
            props_df[(props_df["player"] == p) & (props_df["market"] == m)]["line"].unique()
        )
        line_pick = st.selectbox("Select Line", lines)

        stat = detect_stat(m)

        df_hist = (
            historical_df[
                (historical_df["player"] == p) & (historical_df[stat].notna())
            ]
            .sort_values("game_date")
            .tail(20)
        )

        df_hist["date"] = df_hist["game_date"].dt.strftime("%b %d")
        df_hist["color"] = np.where(df_hist[stat] > line_pick, "green", "red")

        hover = [
            f"<b>{d}</b><br>{stat.upper()}: {v}<br>Opponent: {opp}"
            for d, opp, v in zip(
                df_hist["date"], df_hist["opponent_team"], df_hist[stat]
            )
        ]

        fig = go.Figure()

        fig.add_bar(
            x=df_hist["date"],
            y=df_hist[stat],
            marker_color=df_hist["color"],
            hovertext=hover,
            hoverinfo="text",
        )

        # Label x-axis with dates
        fig.update_xaxes(tickvals=df_hist["date"], ticktext=df_hist["date"])

        # Add logos under bars
        for date_label, opp_team in zip(df_hist["date"], df_hist["opponent_team"]):
            logo_url = TEAM_LOGOS.get(opp_team, "")
            if logo_url:
                fig.add_layout_image(
                    dict(
                        source=logo_url,
                        xref="x",
                        yref="paper",
                        x=date_label,
                        y=-0.15,
                        sizex=0.2,
                        sizey=0.2,
                        xanchor="center",
                        yanchor="top",
                    )
                )

        fig.add_hline(
            y=line_pick,
            line_dash="dash",
            line_color="white",
            annotation_text=f"Line: {line_pick}",
            annotation_position="top left",
        )

        fig.update_layout(
            height=450,
            plot_bgcolor="#222",
            paper_bgcolor="#222",
            font=dict(color="white"),
            margin=dict(b=80, t=40, l=40, r=20),
        )

        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------
# TAB 3 — SAVED BETS
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
# TAB 4 — PROP ANALYTICS
# ------------------------------------------------------
with tab4:
    st.subheader("Prop Analytics")

    d = filter_props(props_df)

    if d.empty:
        st.info("No props match your filters.")
    else:
        d = get_dynamic_averages(d)
        d = add_defense(d)
        d = format_display(d)
        d["Price"] = d["price"].apply(format_moneyline)

        if "ev" not in d.columns:
            st.error("❌ EV column missing from the database.")
            st.stop()

        d["ev"] = pd.to_numeric(d["ev"], errors="coerce")
        d["Hit Rate 10"] = d["hit_rate_last10"]

        d = d.sort_values("ev", ascending=False)

        cols = [
            "player",
            "market",
            "line",
            "Price",
            "bookmaker",
            "ev",
            "Matchup Difficulty",
            "Hit Rate 10",
            "L10 Avg",
        ]

        st.dataframe(
            d[cols],
            use_container_width=True,
            hide_index=True
        )

# ------------------------------------------------------
# LAST UPDATED
# ------------------------------------------------------
now = datetime.now(EST)
st.sidebar.markdown(f"**Last Updated:** {now.strftime('%b %d, %I:%M %p')} ET")
