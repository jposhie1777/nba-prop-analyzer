# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytz
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# ------------------------------------------------------
# TIMEZONE (EST)
# ------------------------------------------------------
EST = pytz.timezone("America/New_York")

# ------------------------------------------------------
# STREAMLIT CONFIG
# ------------------------------------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# Global CSS
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
# SESSION STATE
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

if "active_tab" not in st.session_state:
    st.session_state.active_tab = "🧮 Props Overview"

if "trend_player" not in st.session_state:
    st.session_state.trend_player = None

if "trend_market" not in st.session_state:
    st.session_state.trend_market = None

if "trend_line" not in st.session_state:
    st.session_state.trend_line = None

# ------------------------------------------------------
# UTILITY FUNCTIONS
# ------------------------------------------------------
def format_moneyline(v):
    try:
        v = float(v)
        v_int = int(round(v))
        return f"+{v_int}" if v_int > 0 else str(v_int)
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
        if not stat:
            return np.nan
        col = f"{stat}_last{n}"
        return row.get(col, np.nan)

    df["L5 Avg"] = df.apply(lambda r: pull(r, 5), axis=1)
    df["L10 Avg"] = df.apply(lambda r: pull(r, 10), axis=1)
    df["L20 Avg"] = df.apply(lambda r: pull(r, 20), axis=1)
    return df


def add_defense(df):
    df = df.copy()
    stat_series = df["market"].apply(detect_stat)

    pos_cols = {
        "pts": "opp_pos_pts_rank",
        "reb": "opp_pos_reb_rank",
        "ast": "opp_pos_ast_rank",
        "pra": "opp_pos_pra_rank",
    }

    # UPDATED to match your table
    overall_cols = {
        "pts": "overall_pts_rank",
        "reb": "overall_reb_rank",
        "ast": "overall_ast_rank",
        "pra": "overall_pra_rank",
    }

    df["Pos Def Rank"] = [
        df.loc[i, pos_cols.get(stat_series[i])]
        if pos_cols.get(stat_series[i]) in df.columns
        else ""
        for i in df.index
    ]

    df["Overall Def Rank"] = [
        df.loc[i, overall_cols.get(stat_series[i])]
        if overall_cols.get(stat_series[i]) in df.columns
        else ""
        for i in df.index
    ]

    df["Matchup Difficulty"] = df.get("matchup_difficulty_score", np.nan)
    return df


def format_display(df):
    df = df.copy()

    # Round matchup difficulty for display
    df["Matchup Difficulty"] = df["Matchup Difficulty"].apply(
        lambda x: int(round(x)) if pd.notna(x) else ""
    )

    # Hit rate columns as percentages
    for col in ["hit_rate_last5", "hit_rate_last10", "hit_rate_last20"]:
        def fmt(x):
            if pd.isna(x):
                return ""
            if 0 <= x <= 1:
                return f"{int(round(x * 100))}%"
            return f"{int(round(x))}%"
        df[col] = df[col].apply(fmt)

    # Average columns as 1 decimal
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

    # EV columns may or may not exist yet; keep as-is
    for c in ["ev_last5", "ev_last10", "ev_last20"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

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
st.sidebar.header("Filters")

games_list = (props_df["home_team"] + " vs " + props_df["visitor_team"]).astype(str)
games = ["All games"] + sorted(games_list.unique())
sel_game = st.sidebar.selectbox("Game", games)

players_sidebar = ["All players"] + sorted(
    props_df["player"].fillna("").astype(str).unique()
)
sel_player = st.sidebar.selectbox("Player", players_sidebar)

markets_sidebar = ["All Stats"] + sorted(
    props_df["market"].fillna("").astype(str).unique()
)
sel_market = st.sidebar.selectbox("Market", markets_sidebar)

books = sorted(props_df["bookmaker"].fillna("").astype(str).unique())
default_books = [b for b in books if b.lower() in ("draftkings", "fanduel")] or books
sel_books = st.sidebar.multiselect("Bookmaker", books, default=default_books)

od_min = int(props_df["price"].min())
od_max = int(props_df["price"].max())
sel_odds = st.sidebar.slider("Odds Range", od_min, od_max, (od_min, od_max))

sel_hit10 = st.sidebar.slider("Min Hit Rate L10", 0.0, 1.0, 0.5)

show_only_saved = st.sidebar.checkbox("Show Only Saved Props", value=False)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

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

    # Restrict to saved props if toggle on
    if show_only_saved and st.session_state.saved_bets:
        saved_df = pd.DataFrame(st.session_state.saved_bets)
        d = d.merge(
            saved_df[["player", "market", "line", "bookmaker"]],
            on=["player", "market", "line", "bookmaker"],
            how="inner",
        )

    return d


# ------------------------------------------------------
# TAB NAV (RADIO)
# ------------------------------------------------------
tab_labels = [
    "🧮 Props Overview",
    "📈 Trend Analysis",
    "📋 Saved Bets",
    "📊 Prop Analytics",
]

current_tab = st.radio(
    "View",
    tab_labels,
    index=tab_labels.index(st.session_state.active_tab),
    horizontal=True,
    key="active_tab",
)

# ------------------------------------------------------
# TAB 1 — PROPS OVERVIEW (data_editor + Save checkbox)
# ------------------------------------------------------
if current_tab == "🧮 Props Overview":
    st.subheader("Props Overview")

    d = filter_props(props_df)

    if d.empty:
        st.info("No props match your filters.")
    else:
        d = get_dynamic_averages(d)
        d = add_defense(d)

        # Numeric copy of hit_rate for sorting if needed
        d["hit_rate_last10_num"] = d["hit_rate_last10"]

        # Display fields
        d["Price"] = d["price"].apply(format_moneyline)
        d = format_display(d)

        # Add logo url for info (optional)
        d["Opponent Logo"] = d["opponent_team"].map(TEAM_LOGOS).fillna("")

        # Add Save column default False
        d_display = d.copy()

        # Mark already-saved bets as True
        if st.session_state.saved_bets:
            saved_df = pd.DataFrame(st.session_state.saved_bets)
            key_cols = ["player", "market", "line", "bookmaker"]
            d_display["Save"] = d_display[key_cols].merge(
                saved_df[key_cols].drop_duplicates(),
                on=key_cols,
                how="left",
                indicator=True,
            )["_merge"].eq("both")
        else:
            d_display["Save"] = False

        display_cols = [
            "Save",
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
            "opponent_team",
        ]

        d_display = d_display[display_cols].rename(
            columns={
                "player": "Player",
                "market": "Market",
                "line": "Line",
                "bookmaker": "Book",
                "hit_rate_last5": "Hit L5",
                "hit_rate_last10": "Hit L10",
                "hit_rate_last20": "Hit L20",
                "opponent_team": "Opponent",
            }
        )

        edited = st.data_editor(
            d_display,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_config={
                "Save": st.column_config.CheckboxColumn(
                    "Save Bet", help="Save/unsave this prop"
                )
            },
            key="props_overview_editor",
        )

        # Update saved_bets from edited table
        saved_rows = edited[edited["Save"]].copy()
        if not saved_rows.empty:
            st.session_state.saved_bets = saved_rows[
                ["Player", "Market", "Line", "Price", "Book"]
            ].rename(
                columns={
                    "Player": "player",
                    "Market": "market",
                    "Line": "line",
                    "Price": "price",
                    "Book": "bookmaker",
                }
            ).drop_duplicates().to_dict("records")
        else:
            st.session_state.saved_bets = []

# ------------------------------------------------------
# TAB 2 — TREND ANALYSIS
# ------------------------------------------------------
elif current_tab == "📈 Trend Analysis":
    st.subheader("Trend Analysis")

    all_players = ["(select)"] + sorted(props_df["player"].unique())
    if st.session_state.trend_player in all_players:
        default_p_index = all_players.index(st.session_state.trend_player)
    else:
        default_p_index = 0

    p = st.selectbox("Player", all_players, index=default_p_index)

    if p != "(select)":
        markets = sorted(props_df[props_df["player"] == p]["market"].unique())
        if st.session_state.trend_market in markets:
            default_m_index = markets.index(st.session_state.trend_market)
        else:
            default_m_index = 0

        m = st.selectbox("Market", markets, index=default_m_index)

        lines = sorted(
            props_df[(props_df["player"] == p) & (props_df["market"] == m)][
                "line"
            ].unique()
        )

        if (
            st.session_state.trend_line is not None
            and st.session_state.trend_line in lines
        ):
            default_line_index = list(lines).index(st.session_state.trend_line)
        else:
            default_line_index = 0

        line_pick = st.selectbox("Select Line", lines, index=default_line_index)

        stat = detect_stat(m)
        if not stat:
            st.warning("Unable to detect stat type for this market.")
        else:
            df_hist = (
                historical_df[
                    (historical_df["player"] == p) & (historical_df[stat].notna())
                ]
                .sort_values("game_date")
                .tail(20)
            )

            if df_hist.empty:
                st.info("No historical data available for this player/stat.")
            else:
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

                fig.update_xaxes(tickvals=df_hist["date"], ticktext=df_hist["date"])

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
                    margin=dict(b=40, t=40, l=40, r=20),
                )

                st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------
# TAB 3 — SAVED BETS
# ------------------------------------------------------
elif current_tab == "📋 Saved Bets":
    st.subheader("Saved Bets")

    if not st.session_state.saved_bets:
        st.info("No saved bets yet. Go to Props Overview and check the 'Save Bet' boxes.")
    else:
        df_save = pd.DataFrame(st.session_state.saved_bets)
        st.dataframe(df_save, use_container_width=True, hide_index=True)

        csv = df_save.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv, "saved_bets.csv", "text/csv")

# ------------------------------------------------------
# TAB 4 — PROP ANALYTICS
# ------------------------------------------------------
elif current_tab == "📊 Prop Analytics":
    st.subheader("Prop Analytics")

    d = filter_props(props_df)

    if d.empty:
        st.info("No props match your filters.")
    else:
        d = get_dynamic_averages(d)
        d = add_defense(d)
        d["Price"] = d["price"].apply(format_moneyline)

        ev_cols = ["ev_last5", "ev_last10", "ev_last20"]
        missing_ev = [c for c in ev_cols if c not in d.columns]
        if missing_ev:
            st.error(f"❌ Missing EV columns in database: {', '.join(missing_ev)}")
        else:
            for col in ev_cols:
                d[col] = pd.to_numeric(d[col], errors="coerce")

            d["Hit Rate 10"] = d["hit_rate_last10"]
            d = d.sort_values("ev_last10", ascending=False)

            cols = [
                "player",
                "market",
                "line",
                "Price",
                "bookmaker",
                "ev_last5",
                "ev_last10",
                "ev_last20",
                "Matchup Difficulty",
                "Hit Rate 10",
                "L10 Avg",
            ]

            d_display = d[cols].rename(
                columns={
                    "player": "Player",
                    "market": "Market",
                    "line": "Line",
                    "bookmaker": "Book",
                    "ev_last5": "EV L5",
                    "ev_last10": "EV L10",
                    "ev_last20": "EV L20",
                }
            )

            st.dataframe(d_display, use_container_width=True, hide_index=True)

# ------------------------------------------------------
# LAST UPDATED
# ------------------------------------------------------
now = datetime.now(EST)
st.sidebar.markdown(f"**Last Updated:** {now.strftime('%b %d, %I:%M %p')} ET")
