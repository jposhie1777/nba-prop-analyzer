# ------------------------------------------------------
# 1️⃣ IMPORTS & CONFIG
# ------------------------------------------------------
import os, json, math, datetime, warnings
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

warnings.filterwarnings("ignore", category=RuntimeWarning)
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# ------------------------------------------------------
# 2️⃣ ENVIRONMENT VARIABLES
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
ODDS_SHEET_NAME = os.getenv("ODDS_SHEET_NAME", "")
GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT", "")

if not PROJECT_ID or not GCP_SERVICE_ACCOUNT:
    st.error("❌ Missing environment variables — check Render settings.")
    st.stop()

if st.sidebar.checkbox("🔍 Show env debug", value=False):
    st.sidebar.json({
        "PROJECT_ID": PROJECT_ID,
        "SPREADSHEET_ID": SPREADSHEET_ID,
        "ODDS_SHEET_NAME": ODDS_SHEET_NAME,
        "GCP_SERVICE_ACCOUNT_present": bool(GCP_SERVICE_ACCOUNT),
    })

# ------------------------------------------------------
# 3️⃣ GCP CLIENTS (CACHED)
# ------------------------------------------------------
@st.cache_resource(show_spinner=True)
def get_gcp_clients():
    from google.oauth2 import service_account
    from google.cloud import bigquery
    import gspread
    creds_dict = json.loads(GCP_SERVICE_ACCOUNT)
    base = service_account.Credentials.from_service_account_info(creds_dict)
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = base.with_scopes(scopes)
    bq = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    gc = gspread.authorize(credentials)
    return bq, gc

bq_client, gc = get_gcp_clients()
st.sidebar.success("✅ GCP clients initialized")

try:
    bq_client.query("SELECT 1").result()
    st.sidebar.success("✅ Connected to BigQuery")
except Exception as e:
    st.sidebar.error(f"❌ BigQuery connection failed: {e}")
    st.stop()

try:
    if SPREADSHEET_ID:
        gc.open_by_key(SPREADSHEET_ID)
        st.sidebar.success("✅ Connected to Google Sheets")
    else:
        st.sidebar.warning("⚠️ SPREADSHEET_ID is empty — odds will not load.")
except Exception as e:
    st.sidebar.warning(f"⚠️ Google Sheets connection failed: {e}")

# ------------------------------------------------------
# 4️⃣ REFRESH + CACHE MANAGEMENT
# ------------------------------------------------------
if "force_refresh" not in st.session_state:
    st.session_state["force_refresh"] = False

if st.session_state["force_refresh"]:
    with st.sidebar:
        st.info("♻️ Clearing cache and reloading data...")
    st.cache_data.clear()
    st.session_state.clear()
    st.session_state["force_refresh"] = False
    st.experimental_rerun()

if st.sidebar.button("🔄 Refresh Data"):
    st.session_state["force_refresh"] = True

# ------------------------------------------------------
# 5️⃣ DATA LOADERS
# ------------------------------------------------------
@st.cache_data(ttl=86400, show_spinner=True)
def load_player_stats(bq):
    query = f"""
    WITH stats AS (
        SELECT player AS player_name, team, DATE(game_date) AS game_date,
               CAST(pts AS FLOAT64) AS pts, CAST(reb AS FLOAT64) AS reb,
               CAST(ast AS FLOAT64) AS ast, CAST(stl AS FLOAT64) AS stl,
               CAST(blk AS FLOAT64) AS blk, CAST(pts_reb AS FLOAT64) AS pts_reb,
               CAST(pts_ast AS FLOAT64) AS pts_ast, CAST(reb_ast AS FLOAT64) AS reb_ast,
               CAST(pts_reb_ast AS FLOAT64) AS pra
        FROM `{PROJECT_ID}.nba_data.player_stats`
        UNION ALL
        SELECT player AS player_name, team, DATE(game_date) AS game_date,
               CAST(pts AS FLOAT64) AS pts, CAST(reb AS FLOAT64) AS reb,
               CAST(ast AS FLOAT64) AS ast, CAST(stl AS FLOAT64) AS stl,
               CAST(blk AS FLOAT64) AS blk, CAST(pts_reb AS FLOAT64) AS pts_reb,
               CAST(pts_ast AS FLOAT64) AS pts_ast, CAST(reb_ast AS FLOAT64) AS reb_ast,
               CAST(pts_reb_ast AS FLOAT64) AS pra
        FROM `{PROJECT_ID}.nba_data_2024_2025.player_stats`
    )
    SELECT * FROM stats
    """
    return bq.query(query).to_dataframe()

@st.cache_data(ttl=86400)
def load_games(bq):
    query = f"""
    WITH g AS (
        SELECT CAST(DATE(date) AS DATE) AS game_date, home_team, visitor_team, status
        FROM `{PROJECT_ID}.nba_data.games`
        UNION ALL
        SELECT CAST(DATE(date) AS DATE) AS game_date, home_team, visitor_team, status
        FROM `{PROJECT_ID}.nba_data_2024_2025.games`
    )
    SELECT * FROM g
    """
    return bq.query(query).to_dataframe()

@st.cache_data(ttl=21600)
def load_odds(gc, spreadsheet_id, sheet_name):
    import re
    try:
        ws = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
        records = ws.get_all_records()
        if not records:
            st.warning("⚠️ Odds sheet is empty.")
            return pd.DataFrame()
        df = pd.DataFrame(records)
    except Exception as e:
        st.sidebar.warning(f"⚠️ Could not load Odds sheet: {e}")
        return pd.DataFrame()

    df.columns = [c.lower().strip() for c in df.columns]

    def normalize_market(s):
        if pd.isna(s): return ""
        m = str(s).lower().strip()
        mapping = {
            "player_points_rebounds_assists": "points_rebounds_assists",
            "player_points_rebounds": "points_rebounds",
            "player_points_assists": "points_assists",
            "player_rebounds_assists": "rebounds_assists",
            "player_points": "points", "player_rebounds": "rebounds",
            "player_assists": "assists", "player_steals": "steals",
            "player_blocks": "blocks", "points_rebounds_assists": "points_rebounds_assists",
            "points_rebounds": "points_rebounds", "points_assists": "points_assists",
            "rebounds_assists": "rebounds_assists", "points": "points",
            "rebounds": "rebounds", "assists": "assists", "steals": "steals", "blocks": "blocks"
        }
        for k, v in mapping.items():
            if k in m:
                return v
        return ""

    market_col = next((c for c in ["market", "market_name", "selection", "bet_type", "description"] if c in df.columns), None)
    df["market_norm"] = df[market_col].apply(normalize_market) if market_col else ""

    if "side" not in df.columns:
        df["side"] = ""
    if df["side"].eq("").all():
        for col in ["label", "selection", "description", "market_name"]:
            if col in df.columns:
                temp = df[col].astype(str).str.lower()
                if temp.str.contains("over").any() or temp.str.contains("under").any():
                    df["side"] = np.where(temp.str.contains("over"), "over",
                                           np.where(temp.str.contains("under"), "under", ""))
                    break
    for col in ["bookmaker", "description"]:
        if col not in df.columns:
            df[col] = ""

    st.sidebar.info(f"💰 Loaded {len(df):,} odds rows")
    return df

# ------------------------------------------------------
# 6️⃣ LOAD DATA
# ------------------------------------------------------
with st.spinner("⏳ Loading data..."):
    player_stats = load_player_stats(bq_client)
    games_df = load_games(bq_client)
    odds_df = load_odds(gc, SPREADSHEET_ID, ODDS_SHEET_NAME)

st.sidebar.success("✅ Data ready")

# ------------------------------------------------------
# 7️⃣ ANALYTICS HELPERS
# ------------------------------------------------------
STAT_MAP = {
    "points": "pts", "rebounds": "reb", "assists": "ast",
    "steals": "stl", "blocks": "blk",
    "points_rebounds": "pts_reb", "points_assists": "pts_ast",
    "rebounds_assists": "reb_ast", "points_rebounds_assists": "pra"
}
STAT_LABELS = {
    "points": "Pts", "rebounds": "Reb", "assists": "Ast",
    "steals": "Stl", "blocks": "Blk", "points_rebounds": "Pts+Reb",
    "points_assists": "Pts+Ast", "rebounds_assists": "Reb+Ast",
    "points_rebounds_assists": "PRA"
}

def american_to_decimal(o):
    try:
        o = float(o)
        return 1 + (o / 100.0) if o > 0 else 1 + (100.0 / abs(o))
    except Exception:
        return np.nan

def hit_rate(series, line, n=None):
    s = series if n is None else series.tail(n)
    return np.nan if len(s) == 0 else (s > line).mean()

def format_percentage(v, d=1):
    try:
        v = float(v)
        return f"{v * 100:.{d}f}%" if not pd.isna(v) else "—"
    except Exception:
        return "—"

def format_moneyline(v):
    try:
        v = int(float(v))
        return f"+{v}" if v > 0 else str(v)
    except Exception:
        return "—"

def kelly_fraction(p, dec_odds):
    if np.isnan(p) or np.isnan(dec_odds): return np.nan
    b = dec_odds - 1.0; q = 1 - p
    return max(0, min(1, (b * p - q) / b))

def series_for_player_stat(df, player, stat_key):
    col = STAT_MAP.get(stat_key)
    if not col: return pd.DataFrame(columns=["game_date", "stat"])
    s = df.loc[df["player_name"] == player, ["game_date", col]].dropna().sort_values("game_date")
    s.rename(columns={col: "stat"}, inplace=True)
    return s

def compute_metrics_for_row(df, player, stat_key, line):
    s = series_for_player_stat(df, player, stat_key)
    if s.empty:
        return {k: np.nan for k in
                ["L5", "L10", "L20", "Season", "hit5", "hit10", "hit20", "hit_season",
                 "trend_r", "edge", "rmse10", "z_line"]}
    vals = s["stat"].astype(float)
    avg = vals.mean()
    return {
        "L5": vals.tail(5).mean(), "L10": vals.tail(10).mean(),
        "L20": vals.tail(20).mean(), "Season": avg,
        "hit5": hit_rate(vals, line, 5), "hit10": hit_rate(vals, line, 10),
        "hit20": hit_rate(vals, line, 20), "hit_season": hit_rate(vals, line, None),
        "trend_r": pd.Series(vals.tail(20)).corr(pd.Series(range(1, len(vals.tail(20)) + 1))),
        "edge": avg - line, "rmse10": np.sqrt(np.mean((vals.tail(10) - line) ** 2)),
        "z_line": (line - avg) / (vals.std(ddof=1) or np.nan)
    }

# ------------------------------------------------------
# 8️⃣ BUILD PROPS TABLE
# ------------------------------------------------------
def build_props_table(stats_df, odds_df, games_df, date_filter, game_pick,
                      player_pick, stat_pick, books, odds_range, min_ev, min_hit, min_kelly):
    g_day = games_df.query("game_date == @date_filter")
    if g_day.empty:
        return pd.DataFrame()
    if game_pick and " vs " in game_pick:
        h, a = game_pick.split(" vs ", 1)
        g_day = g_day.query("home_team == @h and visitor_team == @a")
    teams_today = set(g_day["home_team"]) | set(g_day["visitor_team"])
    o = odds_df.copy()
    if "bookmaker" in o and books:
        o = o[o["bookmaker"].isin(books)]
    if "home_team" in o and "away_team" in o:
        o = o[o["home_team"].isin(teams_today) | o["away_team"].isin(teams_today)]
    if stat_pick:
        o = o[o["market_norm"] == stat_pick]
    if player_pick and player_pick != "All players":
        if "description" in o:
            o = o[o["description"].str.lower() == player_pick.lower()]
    if "price" in o:
        o = o[o["price"].between(odds_range[0], odds_range[1])]
    if o.empty:
        return pd.DataFrame()
    rows = []
    for _, r in o.iterrows():
        player, stat_key = str(r.get("description", "")), str(r.get("market_norm", "")).lower()
        line, book, price = r.get("point", np.nan), r.get("bookmaker", ""), r.get("price", np.nan)
        side = str(r.get("side", "")).lower()
        if not player or pd.isna(line) or not stat_key: continue
        m = compute_metrics_for_row(stats_df, player, stat_key, line)
        vals = series_for_player_stat(stats_df, player, stat_key)["stat"]
        if vals.empty: continue
        hit10 = hit_rate(vals, line, 10)
        p_hit = hit10 if side == "over" else (1 - hit10 if side == "under" else np.nan)
        dec = american_to_decimal(price)
        ev = np.nan if np.isnan(p_hit) or np.isnan(dec) else p_hit * (dec - 1) - (1 - p_hit)
        kelly = np.nan if np.isnan(p_hit) or np.isnan(dec) else kelly_fraction(p_hit, dec)
        rows.append({
            "Player": player, "Stat": STAT_LABELS.get(stat_key, stat_key),
            "Bookmaker": book, "Side": side.title() if side in ["over", "under"] else "—",
            "Line": line, "Price (Am)": price, "EV": ev, "Kelly %": kelly,
            **m
        })
    df = pd.DataFrame(rows)
    if df.empty: return df
    if min_ev is not None: df = df[df["EV"] >= min_ev]
    if min_hit is not None: df = df[df["hit10"] >= min_hit]
    if min_kelly is not None: df = df[df["Kelly %"] >= min_kelly]
    return df.sort_values(["EV", "hit10", "Kelly %"], ascending=[False]*3)

# ------------------------------------------------------
# 9️⃣ SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙️ Filters")
today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)

if not games_df.empty:
    day_games = games_df.query("game_date == @sel_date")[["home_team", "visitor_team"]].copy()
    day_games["matchup"] = day_games["home_team"] + " vs " + day_games["visitor_team"]
    game_options = ["All games"] + day_games["matchup"].tolist()
else:
    game_options = ["All games"]
sel_game = st.sidebar.selectbox("Game", game_options)
players_today = sorted(player_stats["player_name"].unique().tolist())
sel_player = st.sidebar.selectbox("Player", ["All players"] + players_today)
sel_stat = st.sidebar.selectbox("Stat", ["All Stats"] + list(STAT_MAP.keys()))
if sel_stat == "All Stats": sel_stat = None
st.sidebar.header("🎲 Odds Filters")
odds_min, odds_max = -1000, 2000
if not odds_df.empty and "price" in odds_df:
    odds_min = int(pd.to_numeric(odds_df["price"], errors="coerce").min())
    odds_max = int(pd.to_numeric(odds_df["price"], errors="coerce").max())
sel_odds_range = st.sidebar.slider("American odds range", odds_min, odds_max, (odds_min, odds_max))
books_available = sorted(odds_df["bookmaker"].dropna().unique().tolist())
sel_books = st.sidebar.multiselect("Bookmakers", books_available, default=books_available)
st.sidebar.header("📈 Analytical Filters")
sel_min_ev = st.sidebar.slider("Min EV", -1.0, 1.0, 0.0, 0.01)
sel_min_hit10 = st.sidebar.slider("Min Hit Rate (L10)", 0.0, 1.0, 0.5, 0.01)
sel_min_kelly = st.sidebar.slider("Min Kelly %", 0.0, 1.0, 0.0, 0.01)

# ------------------------------------------------------
# 🔟 MAIN UI
# ------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets"])

with tab1:
    df = build_props_table(player_stats, odds_df, games_df, pd.to_datetime(sel_date).date(),
                           None if sel_game == "All games" else sel_game,
                           None if sel_player == "All players" else sel_player,
                           sel_stat, sel_books, sel_odds_range,
                           sel_min_ev, sel_min_hit10, sel_min_kelly)
    if df.empty:
        st.info("No props match filters.")
    else:
        df["Price (Am)"] = df["Price (Am)"].apply(format_moneyline)
        df["Kelly %"] = df["Kelly %"].apply(lambda x: format_percentage(x, 1))
        df["EV"] = df["EV"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
        st.dataframe(df, use_container_width=True)

with tab2:
    st.subheader("Trend Analysis")
    players = sorted(player_stats["player_name"].unique().tolist())
    player_pick = st.selectbox("Player", players)
    stat_pick = st.selectbox("Stat", list(STAT_MAP.keys()))
    line_pick = st.number_input("Line", 0.0, 100.0, 10.0, 0.5)
    st.markdown(f"**{player_pick} – {stat_pick} ({line_pick})**")
    s = series_for_player_stat(player_stats, player_pick, stat_pick)
    if not s.empty:
        s = s.sort_values("game_date").tail(20)
        fig = go.Figure()
        fig.add_bar(x=s["game_date"].astype(str), y=s["stat"], marker_color="#4287f5")
        fig.add_hline(y=line_pick, line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for this player/stat.")

with tab3:
    st.subheader("Saved Bets")
    if "saved_bets" not in st.session_state:
        st.session_state.saved_bets = []
    if not st.session_state.saved_bets:
        st.info("No bets saved yet.")
    else:
        saved_df = pd.DataFrame(st.session_state.saved_bets)
        st.dataframe(saved_df, use_container_width=True)
        csv = saved_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Saved Bets CSV", data=csv, file_name="saved_bets.csv", mime="text/csv")
