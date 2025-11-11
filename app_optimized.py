# ------------------------------------------------------
# 1️⃣ IMPORTS & CONFIG
# ------------------------------------------------------
import os, json, time, datetime, math, warnings
import numpy as np
import pandas as pd
import streamlit as st

warnings.filterwarnings("ignore", category=RuntimeWarning)
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

PROJECT_ID = os.getenv("PROJECT_ID", "")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
ODDS_SHEET_NAME = os.getenv("ODDS_SHEET_NAME", "")
GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT", "")

if not PROJECT_ID or not GCP_SERVICE_ACCOUNT:
    st.error("❌ Missing environment variables — check Render settings.")
    st.stop()

# ------------------------------------------------------
# 2️⃣ GCP CLIENTS (CACHED)
# ------------------------------------------------------
@st.cache_resource
def get_gcp_clients():
    from google.oauth2 import service_account
    from google.cloud import bigquery
    import gspread
    creds_dict = json.loads(GCP_SERVICE_ACCOUNT)
    base_credentials = service_account.Credentials.from_service_account_info(creds_dict)
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = base_credentials.with_scopes(scopes)
    bq = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    gc = gspread.authorize(credentials)
    return bq, gc

bq_client, gc = get_gcp_clients()
st.sidebar.success("✅ GCP clients initialized")

# ------------------------------------------------------
# 3️⃣ CACHED DATA LOADERS
# ------------------------------------------------------
CACHE_DIR = "/data" if os.path.exists("/data") else "/tmp"
PLAYER_CACHE = f"{CACHE_DIR}/player_stats.parquet"
ODDS_CACHE = f"{CACHE_DIR}/odds_cache.json"

@st.cache_data(ttl=86400)
def load_player_stats():
    if os.path.exists(PLAYER_CACHE):
        return pd.read_parquet(PLAYER_CACHE)
    query = f"""
    SELECT player AS player_name, team, DATE(game_date) AS game_date,
           CAST(pts AS FLOAT64) AS pts, CAST(reb AS FLOAT64) AS reb,
           CAST(ast AS FLOAT64) AS ast, CAST(stl AS FLOAT64) AS stl,
           CAST(blk AS FLOAT64) AS blk, CAST(pts_reb_ast AS FLOAT64) AS pra
    FROM `{PROJECT_ID}.nba_data_2024_2025.player_stats`
    """
    df = bq_client.query(query).to_dataframe()
    df.to_parquet(PLAYER_CACHE)
    return df

@st.cache_data(ttl=86400)
def load_games():
    return bq_client.query(f"""
    SELECT DATE(date) AS game_date, home_team, visitor_team, status
    FROM `{PROJECT_ID}.nba_data_2024_2025.games`
    """).to_dataframe()

@st.cache_data(ttl=21600)
def load_odds_sheet():
    import gspread
    if os.path.exists(ODDS_CACHE):
        return pd.read_json(ODDS_CACHE)
    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(ODDS_SHEET_NAME)
    df = pd.DataFrame(sheet.get_all_records())
    df.to_json(ODDS_CACHE, orient="records")
    return df

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    for f in [PLAYER_CACHE, ODDS_CACHE]:
        if os.path.exists(f): os.remove(f)
    st.experimental_rerun()

# ------------------------------------------------------
# 4️⃣ ANALYTICS HELPERS (your full original functions)
# ------------------------------------------------------
STAT_MAP = {...}
STAT_LABELS = {...}

def american_to_decimal(odds): ...
def hit_rate(series, line, n=None): ...
def format_percentage(value, decimals=1): ...
def kelly_fraction(p, dec_odds): ...
def trend_pearson_r(series_last20): ...
def rmse_to_line(series, line, n=None): ...
def build_props_table(...): ...
def plot_trend(...): ...
# (Paste all your helper functions exactly as they were here)

# ------------------------------------------------------
# 5️⃣ SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙️ Filters")
today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)
games_df = load_games()

day_games = games_df.query("game_date == @sel_date")[["home_team","visitor_team"]]
day_games["matchup"] = day_games["home_team"] + " vs " + day_games["visitor_team"]
sel_game = st.sidebar.selectbox("Game", ["All games"] + day_games["matchup"].tolist())

with st.spinner("Loading data..."):
    player_stats = load_player_stats()
    odds_df = load_odds_sheet()

st.sidebar.info("🏀 Environment ready")

# ------------------------------------------------------
# 6️⃣ MAIN UI TABS (your original tab logic)
# ------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets", "📊 Prop Analytics"
])

# ----------------------------
# TAB 1 – PROPS OVERVIEW
# ----------------------------
with tab1:
    st.subheader("Props Overview")

    # --- Build the props table ---
    df = build_props_table(
        stats_df,
        odds_df,
        games_df,
        pd.to_datetime(sel_date).date(),
        None if sel_game == "All games" else sel_game,
        None if sel_player == "All players" else sel_player,
        sel_stat,
        sel_books,
        sel_odds_range,
        sel_min_ev,
        sel_min_hit10,
        sel_min_kelly
    )

    if df.empty:
        st.info("No props match your filters.")
    else:
        # --- Format and sanitize numeric data ---
        price_col = "Price (Am)" if "Price (Am)" in df.columns else "Price"
        edge_col = "Edge (Season-Line)" if "Edge (Season-Line)" in df.columns else "Edge"

        df[f"{price_col}_num"] = pd.to_numeric(df[price_col], errors="coerce")
        df = df.dropna(subset=[f"{price_col}_num"])
        df = df[df[f"{price_col}_num"] >= odds_threshold]

        # Format key metrics
        df["Price (Am)"] = df[f"{price_col}_num"].apply(format_moneyline)
        df["Kelly %"] = df["Kelly %"].apply(lambda x: format_percentage(x, 1))
        df["EV"] = pd.to_numeric(df["EV"], errors="coerce").apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
        df["Edge (Season-Line)"] = pd.to_numeric(df.get(edge_col, np.nan), errors="coerce")

        # --- Add numeric columns for percentage sorting ---
        for col in ["Hit5", "Hit10", "Hit20", "Hit Season"]:
            df[f"{col}_num"] = pd.to_numeric(
                df[col]
                .astype(str)
                .str.replace("%", "")
                .str.replace("—", "")
                .replace("", np.nan),
                errors="coerce"
            )

        # --- Display hit % columns with optional counts ---
        for col, n in zip(["Hit5", "Hit10", "Hit20", "Hit Season"], [5, 10, 20, len(df)]):
            if show_hit_counts:
                df[col] = df[col].apply(lambda v: f"{format_percentage(v)} ({format_ratio(v, n)})")
            else:
                df[col] = df[col].apply(format_percentage)

        # --- Add Save Bet checkbox column ---
        df["Save Bet"] = False
        for i, row in df.iterrows():
            key = _bet_key(row)
            if any(_bet_key(x) == key for x in st.session_state.saved_bets):
                df.at[i, "Save Bet"] = True

        if "Stat_key" not in df.columns:
            df["Stat_key"] = ""

        # --- Default column order ---
        ordered_cols = [
            "Save Bet",
            "Player",
            "Stat",
            "Side",
            "Line",
            "Price (Am)",
            "Bookmaker",
            "Hit Season",
            "Hit20",
            "L20 Avg",
            "Hit10",
            "L10 Avg",
            "Hit5",
            "L5 Avg",
        ]

        visible_cols = [c for c in ordered_cols if c in df.columns]
        df_display = df[visible_cols + ["Stat_key"]].copy()

        # --- Persist and reset column order ---
        if "column_order" in st.session_state:
            del st.session_state["column_order"]
        st.session_state["column_order"] = visible_cols

        # --- Header and reset button ---
        st.markdown("### 📊 Player Props")
        cols_reset = st.columns([1, 6])
        with cols_reset[0]:
            if st.button("🔄 Reset Columns"):
                st.session_state["column_order"] = visible_cols

        st.caption(
            "💡 Drag & drop columns to reorder — layout persists for your session. "
            "Check or uncheck 'Save Bet' to track favorites."
        )

        from streamlit import column_config
        col_cfg = {
            "Save Bet": column_config.CheckboxColumn(help="Save or unsave this bet", width="auto"),
            "Player": column_config.TextColumn(width="auto"),
            "Stat": column_config.TextColumn(width="auto"),
            "Side": column_config.TextColumn(width="auto"),
            "Line": column_config.NumberColumn(format="%.1f", width="auto"),
            "Price (Am)": column_config.TextColumn(help="American odds", width="auto"),
            "Bookmaker": column_config.TextColumn(width="auto"),
            "Hit Season": column_config.TextColumn(width="auto"),
            "Hit20": column_config.TextColumn(width="auto"),
            "L20 Avg": column_config.NumberColumn(format="%.1f", width="auto"),
            "Hit10": column_config.TextColumn(width="auto"),
            "L10 Avg": column_config.NumberColumn(format="%.1f", width="auto"),
            "Hit5": column_config.TextColumn(width="auto"),
            "L5 Avg": column_config.NumberColumn(format="%.1f", width="auto"),
        }

        # --- Display Data Editor with numeric sort order for % columns ---
        edited_df = st.data_editor(
            df_display,
            use_container_width=True,
            hide_index=True,
            key="props_editor",
            column_config=col_cfg,
            column_order=st.session_state["column_order"],
            sort_by=[f"{c}_num" for c in ["Hit Season", "Hit20", "Hit10", "Hit5"] if f"{c}_num" in df.columns],
        )

        # --- Save updated column order ---
        new_order = [c for c in edited_df.columns if c != "Stat_key"]
        st.session_state["column_order"] = new_order

        # --- Sync Save Bet state ---
        for i, row in edited_df.iterrows():
            key = _bet_key(row)
            checked = row.get("Save Bet", False)
            exists = any(_bet_key(x) == key for x in st.session_state.saved_bets)
            if checked and not exists:
                st.session_state.saved_bets.append(row.to_dict())
            elif not checked and exists:
                st.session_state.saved_bets = [
                    x for x in st.session_state.saved_bets if _bet_key(x) != key
                ]
# ----------------------------
# TAB 2 – TREND ANALYSIS (Single Stat, Dynamic Line)
# ----------------------------
with tab2:
    st.subheader("Trend Analysis")

    # Handle potential missing odds column
    if "side" not in odds_df.columns:
        odds_df["side"] = ""

    pre_p = st.session_state.get("trend_player")
    pre_k = st.session_state.get("trend_stat_key")
    pre_l = st.session_state.get("trend_line")

    if df is None or df.empty:
        st.info("Load props in the Overview tab first.")
        st.stop()

    # --- Player dropdown ---
    players_in_df = ["(choose)"] + sorted(df["Player"].unique().tolist())
    p_pick = st.selectbox(
        "Player",
        players_in_df,
        index=(players_in_df.index(pre_p) if pre_p in players_in_df else 0)
    )
    if p_pick == "(choose)":
        st.stop()

    # --- Stat type dropdown (single stat at a time) ---
    player_stats_available = df.loc[df["Player"] == p_pick, ["Stat", "Stat_key"]].drop_duplicates()
    if player_stats_available.empty:
        st.warning("No available stats for this player in your current filters.")
        st.stop()

    stat_list = player_stats_available["Stat_key"].tolist()
    stat_pick = st.selectbox(
        "Stat Type (matches odds sheet)",
        stat_list,
        index=(stat_list.index(pre_k) if pre_k in stat_list else 0)
    )

    # --- Line dropdown (dynamic based on selected stat) ---
    lines = sorted(df[(df["Player"] == p_pick) & (df["Stat_key"] == stat_pick)]["Line"].dropna().unique().tolist())
    if not lines:
        st.warning("No available lines for this stat.")
        st.stop()

    default_line_idx = lines.index(pre_l) if (pre_l in lines) else 0
    line_pick = st.selectbox("Book line (threshold)", lines, index=default_line_idx)

    # --- Bet side toggle ---
    side_pick = st.selectbox("Bet side", ["Over", "Under"], index=0)

    # --- Show current odds for that stat/line ---
    price_rows = odds_df[
        (odds_df["description"] == p_pick)
        & (odds_df["market_norm"] == stat_pick)
        & (abs(odds_df["point"] - line_pick) < 0.01)
    ]

    if not price_rows.empty:
        over_price = price_rows.loc[price_rows["side"].str.lower() == "over", "price"].dropna()
        under_price = price_rows.loc[price_rows["side"].str.lower() == "under", "price"].dropna()
        msg = []
        if len(over_price):
            msg.append(f"**Over {line_pick}**: {int(over_price.iloc[0])}")
        if len(under_price):
            msg.append(f"**Under {line_pick}**: {int(under_price.iloc[0])}")
        st.markdown("💰 Current Prices: " + " | ".join(msg))
    else:
        st.markdown("_No recent odds found for this player/stat/line._")

    # --- Chart ---
    st.markdown(f"**Chart:** {p_pick} – {stat_pick} ({side_pick} {line_pick})")
    plot_trend(stats_df, p_pick, stat_pick, line_pick)

# ----------------------------
# TAB 3 – SAVED BETS
# ----------------------------
with tab3:
    st.subheader("Saved Bets")

    saved_bets = st.session_state.get("saved_bets", [])
    if not saved_bets:
        st.info("No bets saved yet. Use the ✅ Save Bet checkbox in the Overview tab.")
    else:
        saved_df = pd.DataFrame(saved_bets)
        if "Stat_key" in saved_df.columns:
            saved_df = saved_df.drop(columns=["Stat_key"], errors="ignore")

        # Display ordered columns cleanly
        preferred = [
            "Player", "Stat", "Side", "Line", "Price (Am)", "Bookmaker",
            "Hit10", "L10 Avg", "Hit20", "L20 Avg", "Hit Season", "Hit5", "L5 Avg"
        ]
        cols = [c for c in preferred if c in saved_df.columns] + [c for c in saved_df.columns if c not in preferred]
        st.dataframe(saved_df[cols], use_container_width=True, hide_index=True)

        csv = saved_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Saved Bets CSV", data=csv, file_name="saved_bets.csv", mime="text/csv")

# ----------------------------
# TAB 4 – PROP ANALYTICS
# ----------------------------
with tab4:
    st.subheader("Prop Analytics")

    df = build_props_table(
        stats_df,
        odds_df,
        games_df,
        pd.to_datetime(sel_date).date(),
        None if sel_game == "All games" else sel_game,
        None if sel_player == "All players" else sel_player,
        sel_stat,
        sel_books,
        sel_odds_range,
        sel_min_ev,
        sel_min_hit10,
        sel_min_kelly
    )

    if df.empty:
        st.info("No props match your filters.")
    else:
        # --- Format core data ---
        df["Price (Am)"] = pd.to_numeric(df.get("Price (Am)", np.nan), errors="coerce").apply(format_moneyline)
        df["EV"] = pd.to_numeric(df.get("EV", np.nan), errors="coerce").apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
        df["Edge"] = pd.to_numeric(df.get("Edge (Season-Line)", np.nan), errors="coerce").apply(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        df["Kelly %"] = df["Kelly %"].apply(lambda x: format_percentage(x, 1))

        # --- Add Save Bet ---
        df["Save Bet"] = False
        for i, row in df.iterrows():
            key = _bet_key(row)
            if any(_bet_key(x) == key for x in st.session_state.saved_bets):
                df.at[i, "Save Bet"] = True

        # --- Desired layout ---
        ordered_cols = [
            "Save Bet",
            "Player",
            "Stat",
            "Side",
            "Line",
            "Price (Am)",
            "Bookmaker",
            "EV",
            "Edge",
            "Trend r",
            "Kelly %",
        ]

        visible_cols = [c for c in ordered_cols if c in df.columns]
        df_display = df[visible_cols].copy()

        if "analytics_order" not in st.session_state:
            st.session_state["analytics_order"] = visible_cols

        from streamlit import column_config
        col_cfg = {
            "Save Bet": column_config.CheckboxColumn(help="Save or unsave bet", width="auto"),
            "Player": column_config.TextColumn(width="auto"),
            "Stat": column_config.TextColumn(width="auto"),
            "Side": column_config.TextColumn(width="auto"),
            "Line": column_config.NumberColumn(format="%.1f", width="auto"),
            "Price (Am)": column_config.TextColumn(help="American odds", width="auto"),
            "Bookmaker": column_config.TextColumn(width="auto"),
            "EV": column_config.TextColumn(help="Expected Value", width="auto"),
            "Edge": column_config.TextColumn(help="Season avg - line", width="auto"),
            "Trend r": column_config.NumberColumn(format="%.2f", help="Recent trend correlation", width="auto"),
            "Kelly %": column_config.TextColumn(help="Kelly fraction", width="auto"),
        }

        edited_df = st.data_editor(
            df_display,
            use_container_width=True,
            hide_index=True,
            key="analytics_editor",
            column_config=col_cfg,
            column_order=st.session_state["analytics_order"],
        )

        # Sync Save Bets between tabs
        for i, row in edited_df.iterrows():
            key = _bet_key(row)
            checked = row.get("Save Bet", False)
            exists = any(_bet_key(x) == key for x in st.session_state.saved_bets)
            if checked and not exists:
                st.session_state.saved_bets.append(row.to_dict())
            elif not checked and exists:
                st.session_state.saved_bets = [x for x in st.session_state.saved_bets if _bet_key(x) != key]
