# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st  # 👈 must be first Streamlit import

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
    st.error("❌ Missing critical environment variables — check PROJECT_ID and GCP_SERVICE_ACCOUNT.")
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
    st.write("✅ Environment variables and credentials loaded successfully!")
except Exception as e:
    st.error(f"❌ Failed to load Google credentials: {e}")
    st.stop()

# ------------------------------------------------------
# INITIALIZE BIGQUERY CLIENT (NO CACHING)
# ------------------------------------------------------
try:
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    _ = bq_client.query("SELECT 1").result()
    st.sidebar.success("✅ Connected to BigQuery")
except Exception as e:
    st.sidebar.error(f"❌ BigQuery connection failed: {e}")
    st.stop()

st.sidebar.info("🏀 Environment setup complete — ready to query data!")

# ------------------------------------------------------
# REDUCED-COLUMN SQL QUERIES
# ------------------------------------------------------
PROPS_SQL = f"""
SELECT
  player, market, line, price, bookmaker,
  player_team, opponent_team, home_team, visitor_team,
  hit_rate_last5, hit_rate_last10, hit_rate_last20,
  pts_last5, reb_last5, ast_last5, pra_last5,
  pts_last10, reb_last10, ast_last10, pra_last10,
  pts_last20, reb_last20, ast_last20, pra_last20,
  stat_stddev_last20, stat_mean_last20,
  decimal_odds, expected_value
FROM `{PROJECT_ID}.{PROP_ANALYZER_DATASET}.{TODAYS_PROPS_TABLE}`
"""

GAME_LOGS_SQL = f"""
SELECT
  player, market, game_date,
  pts, reb, ast, pra,
  line, season_avg,
  team, opponent_team
FROM `{PROJECT_ID}.{PROP_ANALYZER_DATASET}.{GAME_LOGS_TABLE}`
"""

# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------
def format_moneyline(value):
    try:
        val = float(value)
        if pd.isna(val):
            return "—"
        v = int(round(val))
        return f"+{v}" if v > 0 else str(v)
    except (ValueError, TypeError):
        return "—"


def get_stat_base_from_market(market: str) -> str:
    """Detect stat category (pts, reb, ast, pra) from a market string."""
    m = (market or "").lower().strip()

    # PRA must be detected BEFORE pts/reb/ast
    if (
        "points_rebounds_assists" in m
        or "pra" in m
        or "pts_reb_ast" in m
        or "p+r+a" in m
    ):
        return "pra"

    if "assist" in m or "ast" in m:
        return "ast"
    if "rebound" in m or "reb" in m:
        return "reb"
    if "point" in m or "pts" in m:
        return "pts"

    return ""


def add_dynamic_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Add L5/L10/L20 Avg columns for each row, based on market type."""
    if df.empty:
        df["L5 Avg"] = np.nan
        df["L10 Avg"] = np.nan
        df["L20 Avg"] = np.nan
        return df

    df = df.copy()

    def pick_avg(row, horizon):
        base = get_stat_base_from_market(row.get("market", ""))
        if not base:
            return np.nan
        col = f"{base}_last{horizon}"
        return row.get(col, np.nan)

    for h in (5, 10, 20):
        df[f"L{h} Avg"] = df.apply(lambda r: pick_avg(r, h), axis=1)

    return df


# ------------------------------------------------------
# DATA LOADERS (NO STREAMLIT CACHING)
# ------------------------------------------------------

def load_props(_bq_client, query: str) -> pd.DataFrame:
    df = _bq_client.query(query).to_dataframe()
    # Normalize column names / strip
    df.columns = [c.strip() for c in df.columns]

    # Strip string columns
    for col in ["player", "market", "bookmaker", "player_team", "opponent_team", "home_team", "visitor_team"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Ensure numeric types where relevant
    numeric_cols = [
        "line", "price",
        "hit_rate_last5", "hit_rate_last10", "hit_rate_last20",
        "pts_last5", "reb_last5", "ast_last5", "pra_last5",
        "pts_last10", "reb_last10", "ast_last10", "pra_last10",
        "pts_last20", "reb_last20", "ast_last20", "pra_last20",
        "stat_stddev_last20", "stat_mean_last20",
        "decimal_odds", "expected_value",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Fractional hit rates for filtering/sorting (0–1)
    for src, dst in [
        ("hit_rate_last5", "hit5_frac"),
        ("hit_rate_last10", "hit10_frac"),
        ("hit_rate_last20", "hit20_frac"),
    ]:
        if src in df.columns:
            df[dst] = df[src] / 100.0

    # Add dynamic averages based on market
    df = add_dynamic_averages(df)
    return df



def load_game_logs(_bq_client, query: str) -> pd.DataFrame:
    df = _bq_client.query(query).to_dataframe()
    df.columns = [c.strip() for c in df.columns]

    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    numeric_cols = ["line", "pts", "reb", "ast", "pra", "season_avg"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for col in ["player", "market", "team", "opponent_team"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


# ------------------------------------------------------
# REFRESH BUTTON (NO CACHE CLEARING)
# ------------------------------------------------------
refresh_clicked = st.sidebar.button("🔄 Refresh Data")
if refresh_clicked:
    st.sidebar.info("♻️ Refreshing data from BigQuery...")
    for key in ["props_df", "game_logs_df", "last_updated"]:
        st.session_state.pop(key, None)
    st.sidebar.success("✅ Reload triggered!")
    st.rerun()


# ------------------------------------------------------
# LOAD DATA INTO SESSION (NO st.cache)
# ------------------------------------------------------
if "props_df" not in st.session_state:
    with st.spinner("⏳ Loading props from BigQuery..."):
        st.session_state.props_df = load_props(bq_client, PROPS_SQL)
        st.session_state.last_updated = datetime.datetime.now()

if "game_logs_df" not in st.session_state:
    with st.spinner("📈 Loading game logs from BigQuery..."):
        st.session_state.game_logs_df = load_game_logs(bq_client, GAME_LOGS_SQL)
        st.session_state.last_updated = datetime.datetime.now()

props_df = st.session_state.props_df
game_logs_df = st.session_state.game_logs_df

if "last_updated" in st.session_state:
    last_updated_str = st.session_state.last_updated.strftime("%Y-%m-%d %I:%M %p")
    st.sidebar.info(f"🕒 **Data last updated:** {last_updated_str}")


# ------------------------------------------------------
# SESSION STATE (Saved Bets)
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []


def _bet_key(row):
    return f"{row['Player']}|{row['Market']}|{row['Bookmaker']}|{row['Line']}|{row['Price (Am)']}"


def sync_saved_bets_from_editor(edited_df):
    for _, row in edited_df.iterrows():
        key = _bet_key(row)
        checked = row.get("Save Bet", False)
        exists = any(_bet_key(x) == key for x in st.session_state.saved_bets)
        if checked and not exists:
            st.session_state.saved_bets.append(row.to_dict())
        elif not checked and exists:
            st.session_state.saved_bets = [x for x in st.session_state.saved_bets if _bet_key(x) != key]


# ------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙️ Filters")

# Date (not currently used for filtering but kept for UI)
today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)

# Game selector from home/visitor
if not props_df.empty and "home_team" in props_df.columns and "visitor_team" in props_df.columns:
    day_games = props_df[["home_team", "visitor_team"]].dropna().drop_duplicates()
    if not day_games.empty:
        day_games["matchup"] = day_games["home_team"] + " vs " + day_games["visitor_team"]
        game_options = ["All games"] + day_games["matchup"].tolist()
    else:
        game_options = ["All games"]
else:
    game_options = ["All games"]

sel_game = st.sidebar.selectbox("Game", game_options)

# Player selector
if not props_df.empty and "player" in props_df.columns:
    players_filtered = props_df.copy()
    if sel_game != "All games" and " vs " in sel_game:
        home, away = sel_game.split(" vs ", 1)
        players_filtered = players_filtered[
            (players_filtered["home_team"] == home) &
            (players_filtered["visitor_team"] == away)
        ]
    players_today = sorted(players_filtered["player"].dropna().unique().tolist())
else:
    players_today = []

player_options = ["All players"] + players_today
sel_player = st.sidebar.selectbox("Player", player_options)

# Market filter (default All Stats)
st.sidebar.markdown("---")
st.sidebar.header("🎯 Stat / Market Type")

if not props_df.empty and "market" in props_df.columns:
    market_list = sorted(props_df["market"].dropna().unique().tolist())
else:
    market_list = []

stat_options = ["All Stats"] + market_list
sel_stat_display = st.sidebar.selectbox("Market (matches odds)", stat_options, index=0)
sel_stat = None if sel_stat_display == "All Stats" else sel_stat_display

# Table display defaults (kept for future extension)
st.sidebar.markdown("---")
st.sidebar.header("📊 Table Display Options")
default_cols = [
    "Player", "Market", "Line", "Price (Am)", "Bookmaker",
    "Hit L5", "Hit L10", "Hit L20", "L5 Avg", "L10 Avg", "L20 Avg",
]
selected_columns = default_cols

# Odds filters
st.sidebar.markdown("---")
st.sidebar.header("🎲 Odds Filters")

books_available = (
    sorted(props_df["bookmaker"].dropna().unique().tolist())
    if not props_df.empty and "bookmaker" in props_df.columns else []
)
sel_books = st.sidebar.multiselect("Bookmakers", books_available, default=books_available)

if not props_df.empty and "price" in props_df.columns:
    odds_float = pd.to_numeric(props_df["price"], errors="coerce")
    odds_min = int(np.nanmin(odds_float)) if not np.isnan(odds_float.min()) else -1000
    odds_max = int(np.nanmax(odds_float)) if not np.isnan(odds_float.max()) else 2000
else:
    odds_min, odds_max = -1000, 2000

sel_odds_range = st.sidebar.slider("American odds range", odds_min, odds_max, (odds_min, odds_max))
odds_threshold = st.sidebar.number_input(
    "Filter: Show Only Odds Above",
    min_value=-2000,
    max_value=2000,
    value=-600,
    step=50,
)

# Analytical filters
st.sidebar.markdown("---")
st.sidebar.header("📈 Analytical Filters")
sel_min_ev = st.sidebar.slider("Minimum EV", -1.0, 1.0, 0.0, 0.01)
sel_min_hit10 = st.sidebar.slider("Minimum Hit Rate (L10)", 0.0, 1.0, 0.5, 0.01)


# ------------------------------------------------------
# BUILD PROPS TABLE (NO CACHE)
# ------------------------------------------------------

def build_props_table(
    props_df,
    game_pick,
    player_pick,
    stat_pick,
    books,
    odds_range,
    min_ev,
    min_hit10,
):
    if props_df is None or props_df.empty:
        return pd.DataFrame()

    df = props_df.copy()

    # Game filter
    if game_pick and isinstance(game_pick, str) and game_pick != "All games" and " vs " in game_pick:
        home, away = game_pick.split(" vs ", 1)
        df = df[(df["home_team"] == home) & (df["visitor_team"] == away)]

    # Player filter
    if player_pick and player_pick != "All players":
        df = df[df["player"] == player_pick]

    # Market filter
    if stat_pick:
        df = df[df["market"] == stat_pick]

    # Book filter
    if books:
        df = df[df["bookmaker"].isin(books)]

    # Odds range filter
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df[df["price"].between(odds_range[0], odds_range[1])]

    # EV filter
    if "expected_value" in df.columns:
        df = df[df["expected_value"] >= min_ev]

    # Hit10 filter (fractional)
    if "hit10_frac" in df.columns:
        df = df[df["hit10_frac"] >= min_hit10]

    if df.empty:
        return df

    # Build display df
    df_display = pd.DataFrame()
    df_display["Player"] = df["player"]
    df_display["Market"] = df["market"]
    df_display["Line"] = df["line"]
    df_display["Price (Am)"] = df["price"]
    df_display["Bookmaker"] = df["bookmaker"]

    df_display["Hit L5"] = df.get("hit5_frac", df.get("hit_rate_last5", np.nan))
    df_display["Hit L10"] = df.get("hit10_frac", df.get("hit_rate_last10", np.nan))
    df_display["Hit L20"] = df.get("hit20_frac", df.get("hit_rate_last20", np.nan))

    df_display["L5 Avg"] = df["L5 Avg"]
    df_display["L10 Avg"] = df["L10 Avg"]
    df_display["L20 Avg"] = df["L20 Avg"]

    df_display["EV"] = df.get("expected_value", np.nan)

    # Keep raw fractional columns if needed later
    df_display["hit5_frac"] = df_display["Hit L5"]
    df_display["hit10_frac"] = df_display["Hit L10"]
    df_display["hit20_frac"] = df_display["Hit L20"]

    return df_display.reset_index(drop=True)


def get_props_table(
    props_df,
    sel_game,
    sel_player,
    sel_stat,
    sel_books,
    sel_odds_range,
    sel_min_ev,
    sel_min_hit10,
):
    return build_props_table(
        props_df,
        sel_game,
        sel_player,
        sel_stat,
        sel_books,
        sel_odds_range,
        sel_min_ev,
        sel_min_hit10,
    )


# ------------------------------------------------------
# TREND PLOT (NO CACHE)
# ------------------------------------------------------

def get_player_game_log(game_logs_df, player, market):
    if game_logs_df is None or game_logs_df.empty:
        return pd.DataFrame()
    df = game_logs_df.copy()
    df = df[(df["player"] == player) & (df["market"] == market)]
    df = df.dropna(subset=["game_date"]).sort_values("game_date")
    return df.tail(20)


def plot_trend(game_logs_df, player, market, line_value):
    s = get_player_game_log(game_logs_df, player, market)
    if s.empty:
        st.info("No game logs found for this player/market.")
        return

    stat_base = get_stat_base_from_market(market)
    stat_col = {
        "pts": "pts",
        "reb": "reb",
        "ast": "ast",
        "pra": "pra",
    }.get(stat_base, "pra")

    if stat_col not in s.columns:
        st.info(f"No '{stat_col}' column found for this market.")
        return

    y_vals = s[stat_col].astype(float)
    dates = s["game_date"].dt.date.astype(str)

    if "season_avg" in s.columns:
        season_avg = float(s["season_avg"].iloc[-1])
    else:
        season_avg = float(y_vals.mean())

    if line_value is not None and not pd.isna(line_value):
        colors = np.where(y_vals > line_value, "#21c36b", "#e45757")
    else:
        colors = "#21c36b"

    fig = go.Figure()
    fig.add_bar(
        x=dates,
        y=y_vals,
        name=stat_col.upper(),
        marker_color=colors,
    )

    if line_value is not None and not pd.isna(line_value):
        fig.add_scatter(
            x=dates,
            y=[line_value] * len(dates),
            name=f"Line ({line_value})",
            mode="lines",
            line=dict(color="#d9534f", dash="dash"),
        )

    fig.add_scatter(
        x=dates,
        y=[season_avg] * len(dates),
        name=f"Season Avg ({season_avg:.1f})",
        mode="lines",
        line=dict(color="#5cb85c"),
    )

    fig.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=60, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(size=12),
        ),
        xaxis=dict(
            title="Game Date",
            categoryorder="category ascending",
            type="category",
        ),
        yaxis_title=stat_col.upper(),
    )
    st.plotly_chart(fig, use_container_width=True)


# ------------------------------------------------------
# DEBUG
# ------------------------------------------------------
with st.sidebar.expander("🔧 Environment Debug Info"):
    st.write(f"Project: {PROJECT_ID}")
    st.write(f"Dataset: {PROP_ANALYZER_DATASET}")
    st.write(f"Props Table: {TODAYS_PROPS_TABLE}")
    st.write(f"Game Logs Table: {GAME_LOGS_TABLE}")


# ------------------------------------------------------
# TABS (Prop Analytics TAB REMOVED)
# ------------------------------------------------------

tab_labels = ["🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets"]
tab1, tab2, tab3 = st.tabs(tab_labels)


# ------------------------------------------------------
# TAB 1 – PROPS OVERVIEW
# ------------------------------------------------------
with tab1:
    st.subheader("Props Overview")

    df = get_props_table(
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
        # Apply odds threshold
        df["Price_raw"] = pd.to_numeric(df["Price (Am)"], errors="coerce")
        df = df[df["Price_raw"] >= odds_threshold]

        if df.empty:
            st.info("No props remain after applying odds threshold.")
        else:
            # Default sort: Hit L10 descending
            if "Hit L10" in df.columns:
                df = df.sort_values("Hit L10", ascending=False, na_position="last")

            df["Price (Am)"] = df["Price_raw"].apply(format_moneyline)
            df["EV"] = pd.to_numeric(df["EV"], errors="coerce").apply(
                lambda x: f"{x:.3f}" if pd.notna(x) else "—"
            )

            # Ensure hit rates are numeric for % display & proper sorting
            for col in ["Hit L5", "Hit L10", "Hit L20"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Save Bet column
            df["Save Bet"] = False
            for i, row in df.iterrows():
                key = _bet_key(row)
                if any(_bet_key(x) == key for x in st.session_state.saved_bets):
                    df.at[i, "Save Bet"] = True

            ordered_cols = [
                "Save Bet",
                "Player", "Market", "Line", "Price (Am)", "Bookmaker",
                "Hit L5", "Hit L10", "Hit L20",
                "L5 Avg", "L10 Avg", "L20 Avg",
                "EV",
            ]
            visible_cols = [c for c in ordered_cols if c in df.columns]
            df_display = df[visible_cols].copy()

            st.markdown("### 📊 Player Props")
            st.caption("💡 Sorted by Hit Rate L10 (highest first). Click headers to resort.")

            from streamlit import column_config

            col_cfg = {
                "Save Bet": column_config.CheckboxColumn(help="Save or unsave this bet", width="auto"),
                "Player": column_config.TextColumn(width="auto"),
                "Market": column_config.TextColumn(width="auto"),
                "Line": column_config.NumberColumn(format="%.1f", width="auto"),
                "Price (Am)": column_config.TextColumn(help="American odds", width="auto"),
                "Bookmaker": column_config.TextColumn(width="auto"),
                # numeric 0–1 values shown as %
                "Hit L5": column_config.NumberColumn(format="0.0%", width="auto"),
                "Hit L10": column_config.NumberColumn(format="0.0%", width="auto"),
                "Hit L20": column_config.NumberColumn(format="0.0%", width="auto"),
                "L5 Avg": column_config.NumberColumn(format="%.1f", width="auto"),
                "L10 Avg": column_config.NumberColumn(format="%.1f", width="auto"),
                "L20 Avg": column_config.NumberColumn(format="%.1f", width="auto"),
                "EV": column_config.TextColumn(width="auto"),
            }

            edited_df = st.data_editor(
                df_display,
                use_container_width=True,
                hide_index=True,
                key="props_editor",
                column_config=col_cfg,
            )

            sync_saved_bets_from_editor(edited_df)


# ------------------------------------------------------
# TAB 2 – TREND ANALYSIS
# ------------------------------------------------------
with tab2:
    st.subheader("Trend Analysis")

    if props_df is None or props_df.empty:
        st.info("No props data available.")
    elif game_logs_df is None or game_logs_df.empty:
        st.info("No game logs available for trend analysis.")
    else:
        players_in_props = ["(choose)"] + sorted(props_df["player"].dropna().unique().tolist())
        p_pick = st.selectbox("Player", players_in_props, index=0)
        if p_pick == "(choose)":
            st.stop()

        markets_for_player = sorted(
            props_df.loc[props_df["player"] == p_pick, "market"].dropna().unique().tolist()
        )
        if not markets_for_player:
            st.warning("No markets available for this player.")
            st.stop()

        stat_pick = st.selectbox("Market (matches odds)", markets_for_player, index=0)

        lines_for_combo = sorted(
            pd.to_numeric(
                props_df.loc[
                    (props_df["player"] == p_pick) &
                    (props_df["market"] == stat_pick),
                    "line",
                ],
                errors="coerce",
            )
            .dropna()
            .unique()
            .tolist()
        )
        if not lines_for_combo:
            st.warning("No lines available for this player/market.")
            st.stop()

        line_pick = st.selectbox("Book line (threshold)", lines_for_combo, index=0)

        current_rows = props_df[
            (props_df["player"] == p_pick) &
            (props_df["market"] == stat_pick) &
            (abs(props_df["line"] - line_pick) < 0.01)
        ]
        if not current_rows.empty:
            snippets = []
            for _, r in current_rows.iterrows():
                b = r.get("bookmaker", "Book")
                price = format_moneyline(r.get("price", np.nan))
                snippets.append(f"**{b}**: `{price}`")
            st.markdown("💰 Current Prices: " + " | ".join(snippets))
        else:
            st.markdown("_No current prices found for this player/market/line._")

        st.markdown(f"**Chart:** {p_pick} – {stat_pick} (Line {line_pick})")
        plot_trend(game_logs_df, p_pick, stat_pick, line_pick)


# ------------------------------------------------------
# TAB 3 – SAVED BETS
# ------------------------------------------------------
with tab3:
    st.subheader("Saved Bets")

    saved_bets = st.session_state.get("saved_bets", [])
    if not saved_bets:
        st.info("No bets saved yet. Use the ✅ Save Bet checkboxes in the Overview tab.")
    else:
        saved_df = pd.DataFrame(saved_bets)

        preferred = [
            "Player", "Market", "Line", "Price (Am)", "Bookmaker",
            "Hit L5", "Hit L10", "Hit L20",
            "L5 Avg", "L10 Avg", "L20 Avg",
            "EV",
        ]
        cols = [c for c in preferred if c in saved_df.columns] + [
            c for c in saved_df.columns if c not in preferred
        ]
        st.dataframe(saved_df[cols], use_container_width=True, hide_index=True)

        csv = saved_df[cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Download Saved Bets CSV",
            data=csv,
            file_name="saved_bets.csv",
            mime="text/csv",
        )
