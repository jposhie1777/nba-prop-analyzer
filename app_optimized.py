# ------------------------------------------------------
# IMPORTS (lazy imports for faster cold start)
# ------------------------------------------------------
import os, json, time, datetime, math, warnings
import numpy as np
import pandas as pd
import streamlit as st

warnings.filterwarnings("ignore", category=RuntimeWarning)
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# ------------------------------------------------------
# ENVIRONMENT VARIABLES (from Render dashboard)
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
ODDS_SHEET_NAME = os.getenv("ODDS_SHEET_NAME", "")
GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT", "")

if not PROJECT_ID or not GCP_SERVICE_ACCOUNT:
    st.error("❌ Missing environment variables — check Render settings.")
    st.stop()

# ------------------------------------------------------
# GOOGLE AUTH (lazy load for faster startup)
# ------------------------------------------------------
@st.cache_resource
def get_gcp_clients():
    """Create and cache GCP clients once."""
    from google.oauth2 import service_account
    from google.cloud import bigquery
    import gspread

    creds_dict = json.loads(GCP_SERVICE_ACCOUNT)
    base_credentials = service_account.Credentials.from_service_account_info(creds_dict)
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = base_credentials.with_scopes(SCOPES)

    bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    gc = gspread.authorize(credentials)
    return bq_client, gc


bq_client, gc = get_gcp_clients()
st.sidebar.success("✅ GCP clients initialized")

# ------------------------------------------------------
# CACHE LOCATIONS (persistent disk on Render)
# ------------------------------------------------------
CACHE_DIR = "/data" if os.path.exists("/data") else "/tmp"
PLAYER_STATS_CACHE = f"{CACHE_DIR}/player_stats.parquet"
ODDS_CACHE = f"{CACHE_DIR}/odds_cache.json"

# ------------------------------------------------------
# SQL QUERIES (trimmed for readability)
# ------------------------------------------------------
PLAYER_STATS_SQL = f"""
SELECT
  player AS player_name,
  team,
  DATE(game_date) AS game_date,
  CAST(pts AS FLOAT64) AS pts,
  CAST(reb AS FLOAT64) AS reb,
  CAST(ast AS FLOAT64) AS ast,
  CAST(stl AS FLOAT64) AS stl,
  CAST(blk AS FLOAT64) AS blk,
  CAST(pts_reb_ast AS FLOAT64) AS pra
FROM `{PROJECT_ID}.nba_data_2024_2025.player_stats`
"""

GAMES_SQL = f"""
SELECT
  DATE(date) AS game_date,
  home_team,
  visitor_team,
  status
FROM `{PROJECT_ID}.nba_data_2024_2025.games`
"""

# ------------------------------------------------------
# CACHED LOADERS — 3x faster by avoiding API calls
# ------------------------------------------------------
@st.cache_data(ttl=86400)
def load_player_stats():
    """Load player stats from cache or BigQuery."""
    if os.path.exists(PLAYER_STATS_CACHE):
        return pd.read_parquet(PLAYER_STATS_CACHE)

    df = bq_client.query(PLAYER_STATS_SQL).to_dataframe()
    df.to_parquet(PLAYER_STATS_CACHE)
    return df


@st.cache_data(ttl=86400)
def load_odds_sheet():
    """Load Google Sheets odds, cache locally as JSON."""
    import gspread

    if os.path.exists(ODDS_CACHE):
        mtime = os.path.getmtime(ODDS_CACHE)
        if time.time() - mtime < 3600 * 6:  # 6 hours cache
            return pd.read_json(ODDS_CACHE)

    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(ODDS_SHEET_NAME)
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    df.to_json(ODDS_CACHE, orient="records")
    return df


@st.cache_data(ttl=86400)
def load_games():
    return bq_client.query(GAMES_SQL).to_dataframe()


# ------------------------------------------------------
# REFRESH BUTTON (clear cache + force reload)
# ------------------------------------------------------
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    for f in [PLAYER_STATS_CACHE, ODDS_CACHE]:
        if os.path.exists(f): os.remove(f)
    st.sidebar.success("♻️ Cache cleared — refreshing data...")
    st.experimental_rerun()

# ------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙️ Filters")
today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)

# Lazy load only when needed
with st.spinner("Loading data..."):
    games_df = load_games()

# Game selector
day_games = games_df.query("game_date == @sel_date")[["home_team", "visitor_team"]]
day_games["matchup"] = day_games["home_team"] + " vs " + day_games["visitor_team"]
game_options = ["All games"] + day_games["matchup"].tolist()
sel_game = st.sidebar.selectbox("Game", game_options)

# Lazy load player stats only if needed
if st.sidebar.checkbox("Load Player Data (slower)", value=False):
    with st.spinner("Loading player stats..."):
        player_stats = load_player_stats()
else:
    player_stats = pd.DataFrame()

# Odds loaded only if user enables it
if st.sidebar.checkbox("Load Odds Data", value=True):
    with st.spinner("Loading odds..."):
        odds_df = load_odds_sheet()
else:
    odds_df = pd.DataFrame()

st.sidebar.info("🏀 Environment ready")

# ------------------------------------------------------
# MAIN DISPLAY
# ------------------------------------------------------
st.title("🏀 NBA Prop Analyzer (Optimized for Render)")
st.caption("Efficient loading using caching and lazy data fetches.")

tab1, tab2 = st.tabs(["📊 Overview", "📈 Trends"])

# Only render tabs once data is available
with tab1:
    if player_stats.empty or odds_df.empty:
        st.info("Load player stats and odds from the sidebar first.")
    else:
        st.write("✅ Data loaded successfully!")
        st.dataframe(player_stats.head(10))

with tab2:
    st.write("Trend visualization (to be added)")
