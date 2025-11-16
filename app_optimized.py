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
# GLOBAL TIMEZONE (EST)
# ------------------------------------------------------
EST = pytz.timezone("America/New_York")

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
    base_credentials = service_account.Credentials.from_service_account_info(
        creds_dict
    )

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
# TEAM LOGOS
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

    df["L5 Avg"]  = df.apply(lambda r: pick(r, 5),  axis=1)
    df["L10 Avg"] = df.apply(lambda r: pick(r, 10), axis=1)
    df["L20 Avg"] = df.apply(lambda r: pick(r, 20), axis=1)
    return df

def apply_defense_color(val):
    if val in ("", None) or pd.isna(val):
        return "background-color: #444444; color: white;"
    v = int(val)
    if v <= 5: return "background-color: #d9534f; color:white;"
    if v <= 15: return "background-color: #f0ad4e; color:black;"
    if v <= 25: return "background-color: #ffd500; color:black;"
    return "background-color: #5cb85c; color:white;"

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
        df.loc[i, pos_map.get(stat[i])] if pos_map.get(stat[i]) in df.columns else ""
        for i in df.index
    ]

    df["Overall Def Rank"] = [
        df.loc[i, overall_map.get(stat[i])] if overall_map.get(stat[i]) in df.columns else ""
        for i in df.index
    ]

    df["Matchup Difficulty"] = df.get("matchup_difficulty_score", np.nan)

    return df

def format_overview_fields(df):
    df = df.copy()

    # Difficulty → whole number
    df["Matchup Difficulty"] = df["Matchup Difficulty"].apply(
        lambda x: f"{int(round(x))}" if pd.notna(x) else ""
    )

    # Hit Rates → correct %
    for col in ["hit_rate_last5", "hit_rate_last10", "hit_rate_last20"]:
        def fmt(x):
            if pd.isna(x): return ""
            if 0 <= x <= 1: return f"{int(round(x * 100))}%"
            return f"{int(round(x))}%"
        df[col] = df[col].apply(fmt)

    # Averages → 1 decimal
    for col in ["L5 Avg", "L10 Avg", "L20 Avg"]:
        df[col] = df[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")

    return df

# ------------------------------------------------------
# LOAD DATA (CONVERT ALL GAME DATES TO EST)
# ------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_props():
    df = bq_client.query(PROPS_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["game_date"] = (
        pd.to_datetime(df["game_date"], errors="coerce")
        .dt.tz_localize("UTC")
        .dt.tz_convert(EST)
    )
    return df

@st.cache_data(show_spinner=True)
def load_historical():
    df = bq_client.query(HISTORICAL_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["game_date"] = (
        pd.to_datetime(df["game_date"], errors="coerce")
        .dt.tz_localize("UTC")
        .dt.tz_convert(EST)
    )
    return df

props_df = load_props()
historical_df = load_historical()

# ------------------------------------------------------
# SIDEBAR
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

st.sidebar.header("⚙ Filters")

games = ["All games"] + sorted(
    (props_df["home_team"] + " vs " + props_df["visitor_team"]).unique()
)
sel_game = st.sidebar.selectbox("Game", games)

players = ["All players"] + sorted(props_df["player"].unique())
sel_player = st.sidebar.selectbox("Player", players)

markets = ["All Stats"] + sorted(props_df["market"].unique())
sel_market = st.sidebar.selectbox("Market", markets)

books = sorted(props_df["bookmaker"].unique())
default_books = [b for b in books if b.lower() in ("draftkings", "fanduel")] or books
sel_books = st.sidebar.multiselect("Bookmaker", books, default=default_books)

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
        d = add_defensive_matchups(d)
        d["Price"] = d["price"].apply(format_moneyline)
        d = format_overview_fields(d)

        d["Opponent Logo"] = d["opponent_team"].apply(
            lambda t: TEAM_LOGOS.get(t, "")
        )

        d = d.sort_values("hit_rate_last10", ascending=False)

        display_cols = [
            "player", "market", "line", "Price", "bookmaker",
            "Pos Def Rank", "Overall Def Rank", "Matchup Difficulty",
            "hit_rate_last5", "hit_rate_last10", "hit_rate_last20",
            "L5 Avg", "L10 Avg", "L20 Avg"
        ]

        html = "<table style='width:100%; border-collapse:collapse;'>"

        html += "<tr>"
        for col in display_cols + ["Opponent Logo"]:
            html += f"<th style='padding:6px; text-align:center; border-bottom:1px solid #444;'>{col}</th>"
        html += "</tr>"

        for _, row in d.iterrows():
            html += "<tr>"
            for col in display_cols:
                val = row[col]
                style = ""
                if col in ["Pos Def Rank", "Overall Def Rank", "Matchup Difficulty"]:
                    style = apply_defense_color(val)
                html += f"<td style='padding:6px; text-align:center; {style}'>{val}</td>"

            logo = TEAM_LOGOS.get(row["opponent_team"], "")
            html += f"<td style='text-align:center;'><img src='{logo}' width='32'></td>"

            html += "</tr>"

        html += "</table>"

        st.markdown(html, unsafe_allow_html=True)

# ------------------------------------------------------
# TAB 2 — TREND ANALYSIS
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
        )
        line_pick = st.selectbox("Select Line", line_values)

        stat = detect_stat(m)

        df_hist = historical_df[
            (historical_df["player"] == p) & (historical_df[stat].notna())
        ].sort_values("game_date").tail(20)

        df_hist["date_str"] = df_hist["game_date"].dt.strftime("%b %d")
        df_hist["color"] = np.where(df_hist[stat] > line_pick, "green", "red")

        hover_text = [
            f"<b>{d}</b><br>{stat.upper()}: {v}<br>Opponent: {opp}"
            for d, opp, v in zip(df_hist["date_str"], df_hist["opponent_team"], df_hist[stat])
        ]

        fig = go.Figure()

        fig.add_bar(
            x=df_hist["date_str"],
            y=df_hist[stat],
            marker_color=df_hist["color"],
            hovertext=hover_text,
            hoverinfo="text",
        )

        # Add Logos under bars
        fig.update_xaxes(
            tickvals=df_hist["date_str"],
            ticktext=[
                f"<img src='{TEAM_LOGOS.get(team, '')}' width='24'>"
                for team in df_hist["opponent_team"]
            ]
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
            xaxis=dict(type="category", tickfont=dict(size=14)),
            plot_bgcolor="#222222",
            paper_bgcolor="#222222",
            font=dict(color="white")
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
# LAST UPDATED (EST)
# ------------------------------------------------------
now_est = datetime.now(EST)
st.sidebar.markdown(
    f"**Last Updated:** {now_est.strftime('%b %d, %I:%M %p')} ET"
)
