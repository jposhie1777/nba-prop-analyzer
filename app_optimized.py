# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st  # must be first streamlit import

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
    ]
    credentials = base_credentials.with_scopes(SCOPES)
    st.write("✅ Google credentials loaded successfully")
except Exception as e:
    st.error(f"❌ Failed to load Google credentials: {e}")
    st.stop()


# ------------------------------------------------------
# INITIALIZE BIGQUERY CLIENT
# ------------------------------------------------------
@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


bq_client = get_bq_client()

try:
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
FROM `{PROJECT_ID}.{PROP_ANALYZER_DATASET}.{TODAYS_PROPS_TABLE}`
"""

GAME_LOGS_SQL = f"""
SELECT *
FROM `{PROJECT_ID}.{PROP_ANALYZER_DATASET}.{GAME_LOGS_TABLE}`
"""


# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------
def format_moneyline(value):
    try:
        v = int(round(float(value)))
        return f"+{v}" if v > 0 else str(v)
    except Exception:
        return "—"


def get_stat_base_from_market(market):
    m = (market or "").lower()
    if any(x in m for x in ["points_rebounds_assists", "pra", "pts_reb_ast", "p+r+a"]):
        return "pra"
    if "assist" in m or "ast" in m:
        return "ast"
    if "rebound" in m or "reb" in m:
        return "reb"
    if "point" in m or "pts" in m:
        return "pts"
    return ""


def add_dynamic_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add L5/L10/L20 rolling averages based on underlying *_lastN columns.
    Operates in-place on df and returns it.
    """
    if df.empty:
        for h in (5, 10, 20):
            col_name = f"L{h} Avg"
            if col_name not in df.columns:
                df[col_name] = np.nan
        return df

    def pick_avg(row, horizon):
        base = get_stat_base_from_market(row.get("market", ""))
        return row.get(f"{base}_last{horizon}", np.nan)

    for h in (5, 10, 20):
        col_name = f"L{h} Avg"
        if col_name not in df.columns:
            df[col_name] = df.apply(lambda r: pick_avg(r, h), axis=1).astype("float32")

    return df


# ------------------------------------------------------
# CACHED LOADERS (MEMORY OPTIMIZED)
# ------------------------------------------------------
@st.cache_data(ttl=600, max_entries=2, show_spinner=True)
def load_props_cached(_bq_client, query):
    # Load once from BigQuery
    raw_df = _bq_client.query(query).to_dataframe()

    # Only keep the columns we actually use in the app
    needed_cols = [
        "player",
        "market",
        "bookmaker",
        "player_team",
        "opponent_team",
        "home_team",
        "visitor_team",
        "line",
        "price",
        "expected_value",
        "hit_rate_last5",
        "hit_rate_last10",
        "hit_rate_last20",
        "pts_last5",
        "reb_last5",
        "ast_last5",
        "pra_last5",
        "pts_last10",
        "reb_last10",
        "ast_last10",
        "pra_last10",
        "pts_last20",
        "reb_last20",
        "ast_last20",
        "pra_last20",
    ]
    cols_present = [c for c in needed_cols if c in raw_df.columns]
    df = raw_df[cols_present].copy()
    df.columns = [c.strip() for c in df.columns]

    # string columns → category to reduce memory
    for col in ["player", "market", "bookmaker", "player_team", "opponent_team", "home_team", "visitor_team"]:
        if col in df:
            df[col] = df[col].astype(str).str.strip().astype("category")

    # numeric conversion with downcasting
    numeric_cols = [
        "line",
        "price",
        "expected_value",
        "hit_rate_last5",
        "hit_rate_last10",
        "hit_rate_last20",
        "pts_last5",
        "reb_last5",
        "ast_last5",
        "pra_last5",
        "pts_last10",
        "reb_last10",
        "ast_last10",
        "pra_last10",
        "pts_last20",
        "reb_last20",
        "ast_last20",
        "pra_last20",
    ]
    for c in numeric_cols:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")

    # fractional hit rates (float32)
    for src, dst in [
        ("hit_rate_last5", "hit5_frac"),
        ("hit_rate_last10", "hit10_frac"),
        ("hit_rate_last20", "hit20_frac"),
    ]:
        if src in df:
            df[dst] = (df[src] / 100.0).astype("float32")

    # dynamic averages (in-place)
    df = add_dynamic_averages(df)

    return df


@st.cache_data(ttl=600, max_entries=2, show_spinner=True)
def load_game_logs_cached(_bq_client, query):
    raw_df = _bq_client.query(query).to_dataframe()

    needed_cols = [
        "game_date",
        "player",
        "market",
        "team",
        "opponent_team",
        "line",
        "pts",
        "reb",
        "ast",
        "pra",
        "season_avg",
    ]
    cols_present = [c for c in needed_cols if c in raw_df.columns]
    df = raw_df[cols_present].copy()
    df.columns = [c.strip() for c in df.columns]

    if "game_date" in df:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    for c in ["line", "pts", "reb", "ast", "pra", "season_avg"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")

    for col in ["player", "market", "team", "opponent_team"]:
        if col in df:
            df[col] = df[col].astype(str).str.strip().astype("category")

    return df


# ------------------------------------------------------
# REFRESH DATA BUTTON
# ------------------------------------------------------
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()


# ------------------------------------------------------
# LOAD DATA (NO SESSION_STATE)
# ------------------------------------------------------
props_df = load_props_cached(bq_client, PROPS_SQL)
game_logs_df = load_game_logs_cached(bq_client, GAME_LOGS_SQL)

last_updated = datetime.datetime.now()
st.sidebar.info(f"🕒 Data updated: {last_updated:%Y-%m-%d %I:%M %p}")


# ------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙️ Filters")

today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)  # currently not used but harmless

# game selector
if not props_df.empty and {"home_team", "visitor_team"}.issubset(props_df.columns):
    day_games = props_df[["home_team", "visitor_team"]].dropna().drop_duplicates()
else:
    day_games = pd.DataFrame(columns=["home_team", "visitor_team"])

if not day_games.empty:
    day_games = day_games.copy()
    day_games["matchup"] = day_games["home_team"].astype(str) + " vs " + day_games["visitor_team"].astype(str)
    game_options = ["All games"] + day_games["matchup"].tolist()
else:
    game_options = ["All games"]

sel_game = st.sidebar.selectbox("Game", game_options)

# player selector
if "player" in props_df.columns:
    players_today = ["All players"] + sorted(pd.Series(props_df["player"].astype(str).unique()).dropna().tolist())
else:
    players_today = ["All players"]
sel_player = st.sidebar.selectbox("Player", players_today)

# stat/market filter
if "market" in props_df.columns:
    market_list = sorted(pd.Series(props_df["market"].astype(str).dropna().unique()).tolist())
else:
    market_list = []
sel_stat_display = st.sidebar.selectbox("Market", ["All Stats"] + market_list)
sel_stat = None if sel_stat_display == "All Stats" else sel_stat_display

# bookmaker filter
if "bookmaker" in props_df.columns:
    books_available = sorted(pd.Series(props_df["bookmaker"].astype(str).dropna().unique()).tolist())
else:
    books_available = []
sel_books = st.sidebar.multiselect("Bookmakers", books_available, default=books_available)

# odds filters
if "price" in props_df.columns and not props_df.empty:
    odds_arr = pd.to_numeric(props_df["price"], errors="coerce")
    odds_min = int(np.nanmin(odds_arr)) if np.isfinite(np.nanmin(odds_arr)) else -2000
    odds_max = int(np.nanmax(odds_arr)) if np.isfinite(np.nanmax(odds_arr)) else 2000
else:
    odds_min, odds_max = -2000, 2000

sel_odds_range = st.sidebar.slider(
    "American odds range",
    odds_min,
    odds_max,
    (odds_min, odds_max),
)
odds_threshold = st.sidebar.number_input("Filter: Show Only Odds Above", -2000, 2000, -600, 50)

# analytical
sel_min_ev = st.sidebar.slider("Minimum EV", -1.0, 1.0, 0.0, 0.01)
sel_min_hit10 = st.sidebar.slider("Minimum Hit Rate (L10)", 0.0, 1.0, 0.5, 0.01)


# ------------------------------------------------------
# MEMORY-OPTIMIZED PROPS TABLE BUILDER (NO CACHE)
# ------------------------------------------------------
def build_props_table(df, game_pick, player_pick, stat_pick, books, odds_range, min_ev, min_hit10):
    if df.empty:
        return pd.DataFrame()

    view = df

    # game filter
    if isinstance(game_pick, str) and " vs " in game_pick:
        home, away = game_pick.split(" vs ", 1)
        if {"home_team", "visitor_team"}.issubset(view.columns):
            view = view[(view["home_team"] == home) & (view["visitor_team"] == away)]

    # player
    if player_pick and player_pick != "All players" and "player" in view.columns:
        view = view[view["player"] == player_pick]

    # market
    if stat_pick and "market" in view.columns:
        view = view[view["market"] == stat_pick]

    # book
    if books and "bookmaker" in view.columns:
        view = view[view["bookmaker"].isin(books)]

    # odds
    if "price" in view.columns:
        view = view[
            (view["price"] >= odds_range[0]) &
            (view["price"] <= odds_range[1])
        ]

    # ev
    if "expected_value" in view.columns:
        view = view[view["expected_value"] >= min_ev]

    # hit rate
    if "hit10_frac" in view.columns:
        view = view[view["hit10_frac"] >= min_hit10]

    if view.empty:
        return pd.DataFrame()

    display_cols = [
        "player",
        "market",
        "line",
        "price",
        "bookmaker",
        "hit5_frac",
        "hit10_frac",
        "hit20_frac",
        "L5 Avg",
        "L10 Avg",
        "L20 Avg",
        "expected_value",
    ]
    display_cols_present = [c for c in display_cols if c in view.columns]

    df_display = view[display_cols_present].copy()

    rename_map = {
        "player": "Player",
        "market": "Market",
        "line": "Line",
        "price": "Price (Am)",
        "bookmaker": "Bookmaker",
        "hit5_frac": "Hit L5",
        "hit10_frac": "Hit L10",
        "hit20_frac": "Hit L20",
        "expected_value": "EV",
    }
    df_display.rename(columns={k: v for k, v in rename_map.items() if k in df_display.columns}, inplace=True)

    return df_display.reset_index(drop=True)


# ------------------------------------------------------
# TABS
# ------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(["🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets", "📊 Prop Analytics"])


# ------------------------------------------------------
# TAB 1 – PROPS OVERVIEW
# ------------------------------------------------------
with tab1:
    st.subheader("Props Overview")

    df = build_props_table(
        props_df,
        sel_game,
        None if sel_player == "All players" else sel_player,
        sel_stat,
        sel_books,
        sel_odds_range,
        sel_min_ev,
        sel_min_hit10,
    )

    if df.empty:
        st.info("No props match your filters.")
    else:
        # work on a small copy for UI formatting only
        df = df.copy()
        df["Price_raw"] = pd.to_numeric(df["Price (Am)"], errors="coerce")
        df = df[df["Price_raw"] >= odds_threshold]

        if df.empty:
            st.info("No props remain after odds threshold.")
        else:
            df = df.sort_values("Hit L10", ascending=False, na_position="last")
            df["Price (Am)"] = df["Price_raw"].apply(format_moneyline)
            if "EV" in df.columns:
                df["EV"] = df["EV"].apply(lambda x: f"{float(x):.3f}" if pd.notna(x) else "—")

            st.dataframe(df, use_container_width=True, hide_index=True)


# ------------------------------------------------------
# TAB 2 – TREND ANALYSIS
# ------------------------------------------------------
def get_player_game_log(df, player, market):
    if df.empty:
        return df

    subset = df[(df["player"] == player) & (df["market"] == market)]
    if "game_date" in subset.columns:
        subset = subset.dropna(subset=["game_date"]).sort_values("game_date")
    return subset.tail(20)


with tab2:
    st.subheader("Trend Analysis")

    if game_logs_df.empty:
        st.info("No game logs available.")
    else:
        if "player" in props_df.columns:
            players = ["(choose)"] + sorted(pd.Series(props_df["player"].astype(str).dropna().unique()).tolist())
        else:
            players = ["(choose)"]

        p_pick = st.selectbox("Player", players)
        if p_pick == "(choose)":
            st.stop()

        # markets for this player
        markets_series = props_df[props_df["player"] == p_pick]["market"] if "market" in props_df.columns else pd.Series([])
        markets = sorted(markets_series.astype(str).dropna().unique().tolist())
        if not markets:
            st.warning("No markets for this player.")
            st.stop()

        stat_pick = st.selectbox("Market", markets)

        # possible lines for this player/market
        line_series = props_df[(props_df["player"] == p_pick) &
                               (props_df["market"] == stat_pick)]["line"] if "line" in props_df.columns else pd.Series([])
        lines = sorted(pd.to_numeric(line_series, errors="coerce").dropna().unique().tolist())
        if not lines:
            st.warning("No lines for this player/market.")
            st.stop()

        line_pick = st.selectbox("Line", lines)

        log_df = get_player_game_log(game_logs_df, p_pick, stat_pick)
        if log_df.empty:
            st.info("No logs found.")
            st.stop()

        stat_base = get_stat_base_from_market(stat_pick)
        stat_col = {"pts": "pts", "reb": "reb", "ast": "ast", "pra": "pra"}.get(stat_base, "pra")

        if stat_col not in log_df.columns:
            st.info(f"No stat column '{stat_col}' available for this market.")
            st.stop()

        fig = go.Figure()
        fig.add_bar(
            x=log_df["game_date"].dt.date.astype(str),
            y=log_df[stat_col],
            marker_color=np.where(log_df[stat_col] > line_pick, "#21c36b", "#e45757"),
        )
        fig.add_hline(y=line_pick, line_dash="dash", line_color="#d9534f")
        st.plotly_chart(fig, use_container_width=True)


# ------------------------------------------------------
# TAB 3 – SAVED BETS
# ------------------------------------------------------
with tab3:
    st.subheader("Saved Bets")
    st.info("Saving bets functionality removed temporarily for optimization. Will reintroduce with low-memory version.")


# ------------------------------------------------------
# TAB 4 – PROP ANALYTICS
# ------------------------------------------------------
with tab4:
    st.subheader("Prop Analytics")

    df = build_props_table(
        props_df,
        sel_game,
        None if sel_player == "All players" else sel_player,
        sel_stat,
        sel_books,
        sel_odds_range,
        sel_min_ev,
        sel_min_hit10,
    )

    if df.empty:
        st.info("No props available.")
    else:
        df = df.copy()
        df["Price_raw"] = pd.to_numeric(df["Price (Am)"], errors="coerce")
        df = df[df["Price_raw"] >= odds_threshold]

        if not df.empty:
            df = df.sort_values("Hit L10", ascending=False)
            df["Price (Am)"] = df["Price_raw"].apply(format_moneyline)
            if "EV" in df.columns:
                df["EV"] = df["EV"].apply(lambda x: f"{float(x):.3f}" if pd.notna(x) else "—")
            st.dataframe(df, use_container_width=True, hide_index=True)
