# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
import math
import time
import datetime
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st  # 👈 must be first Streamlit import
import gspread
from google.oauth2 import service_account
from google.cloud import bigquery
import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning)

# ------------------------------------------------------
# STREAMLIT PAGE CONFIG
# ------------------------------------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# ------------------------------------------------------
# ENVIRONMENT VARIABLES (Render, Streamlit Cloud, or local .env)
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "nba")
ODDS_TABLE = os.getenv("ODDS_TABLE", "nba_odds")

# Optional player stats datasets
PLAYER_STATS_DATASET = os.getenv("PLAYER_STATS_DATASET", "nba_data")
PLAYER_STATS_DATASET_2 = os.getenv("PLAYER_STATS_DATASET_2", "nba_data_2024_2025")

# Optional: keep Sheets around (commented odds loader below)
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
ODDS_SHEET_NAME = os.getenv("ODDS_SHEET_NAME", "")
GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT", "")

# Validate required environment
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
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = base_credentials.with_scopes(SCOPES)
    st.write("✅ Environment variables and credentials loaded successfully!")
except Exception as e:
    st.error(f"❌ Failed to load Google credentials: {e}")
    st.stop()

# ------------------------------------------------------
# INITIALIZE CLIENTS (CACHED)
# ------------------------------------------------------
@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

bq_client = get_bq_client()

try:
    st.sidebar.success("✅ Connected to BigQuery")
except Exception as e:
    st.sidebar.error(f"❌ BigQuery connection failed: {e}")
    st.stop()

try:
    gc = gspread.authorize(credentials)
    if SPREADSHEET_ID:
        _ = gc.open_by_key(SPREADSHEET_ID)  # test access (safe)
    st.sidebar.success("✅ Connected to Google Sheets")
except Exception as e:
    st.sidebar.warning(f"⚠️ Google Sheets connection failed: {e}")

st.sidebar.info("🏀 Environment setup complete — ready to query data!")

# ------------------------------------------------------
# SQL QUERIES
# (keeping inline for single-file; could be moved to separate module)
# ------------------------------------------------------
PLAYER_STATS_SQL = f"""
WITH stats AS (
  SELECT
    player AS player_name,
    team,
    CAST(DATE(game_date) AS DATE) AS game_date,
    CAST(min AS FLOAT64) AS minutes,
    CAST(pts AS FLOAT64) AS pts,
    CAST(reb AS FLOAT64) AS reb,
    CAST(ast AS FLOAT64) AS ast,
    CAST(stl AS FLOAT64) AS stl,
    CAST(blk AS FLOAT64) AS blk,
    CAST(pts_reb AS FLOAT64) AS pts_reb,
    CAST(pts_ast AS FLOAT64) AS pts_ast,
    CAST(reb_ast AS FLOAT64) AS reb_ast,
    CAST(pts_reb_ast AS FLOAT64) AS pra
  FROM `{PROJECT_ID}.{PLAYER_STATS_DATASET}.player_stats`
  UNION ALL
  SELECT
    player AS player_name,
    team,
    CAST(DATE(game_date) AS DATE) AS game_date,
    CAST(min AS FLOAT64) AS minutes,
    CAST(pts AS FLOAT64) AS pts,
    CAST(reb AS FLOAT64) AS reb,
    CAST(ast AS FLOAT64) AS ast,
    CAST(stl AS FLOAT64) AS stl,
    CAST(blk AS FLOAT64) AS blk,
    CAST(pts_reb AS FLOAT64) AS pts_reb,
    CAST(pts_ast AS FLOAT64) AS pts_ast,
    CAST(reb_ast AS FLOAT64) AS reb_ast,
    CAST(pts_reb_ast AS FLOAT64) AS pra
  FROM `{PROJECT_ID}.{PLAYER_STATS_DATASET_2}.player_stats`
)
SELECT * FROM stats
"""

GAMES_SQL = f"""
WITH g AS (
  SELECT
    CAST(game_id AS STRING) AS game_id,
    CAST(DATE(date) AS DATE) AS game_date,
    home_team,
    visitor_team,
    status
  FROM `{PROJECT_ID}.{PLAYER_STATS_DATASET}.games`
  UNION ALL
  SELECT
    CAST(game_id AS STRING) AS game_id,
    CAST(DATE(date) AS DATE) AS game_date,
    home_team,
    visitor_team,
    status
  FROM `{PROJECT_ID}.{PLAYER_STATS_DATASET_2}.games`
)
SELECT * FROM g
"""
ODDS_SQL = f"""
SELECT
  CAST(game_id AS STRING) AS game_id,

  -- Parse MM/DD/YYYY into a valid timestamp
  TIMESTAMP( PARSE_DATETIME('%m/%d/%Y', commence_time) ) AS commence_time,

  in_play,
  bookmaker,
  last_update,
  home_team,
  away_team,
  market,
  label,
  description,
  CAST(price AS FLOAT64) AS price,
  CAST(point AS FLOAT64) AS point
FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{ODDS_TABLE}`
"""


# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------
STAT_MAP = {
    "points": "pts",
    "rebounds": "reb",
    "assists": "ast",
    "steals": "stl",
    "blocks": "blk",
    "points_rebounds": "pts_reb",
    "points_assists": "pts_ast",
    "rebounds_assists": "reb_ast",
    "points_rebounds_assists": "pra",
}

STAT_LABELS = {
    "points": "Pts",
    "rebounds": "Reb",
    "assists": "Ast",
    "steals": "Stl",
    "blocks": "Blk",
    "points_rebounds": "Pts+Reb",
    "points_assists": "Pts+Ast",
    "rebounds_assists": "Reb+Ast",
    "points_rebounds_assists": "PRA",
}

MARKET_NORMALIZE_MAP = {
    "player_points_rebounds_assists": "points_rebounds_assists",
    "player_points_rebounds": "points_rebounds",
    "player_points_assists": "points_assists",
    "player_rebounds_assists": "rebounds_assists",
    "player_points": "points",
    "player_rebounds": "rebounds",
    "player_assists": "assists",
    "player_steals": "steals",
    "player_blocks": "blocks",
    # common variations
    "points_rebounds_assists": "points_rebounds_assists",
    "points_rebounds": "points_rebounds",
    "points_assists": "points_assists",
    "rebounds_assists": "rebounds_assists",
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "steals": "steals",
    "blocks": "blocks",
}

def normalize_market_series(market_series: pd.Series) -> pd.Series:
    """Vectorized market normalization using substring matching."""
    s = market_series.astype(str).str.lower().fillna("")
    result = pd.Series("", index=s.index, dtype="object")
    # loop keys (small) but use vectorized contains over series (fast)
    for k, v in MARKET_NORMALIZE_MAP.items():
        mask = s.str.contains(k)
        result[mask] = v
    return result

def american_to_decimal(odds):
    try:
        o = float(odds)
    except Exception:
        return np.nan
    return 1 + (o / 100.0) if o > 0 else 1 + (100.0 / abs(o))

def hit_rate(series, line, n=None):
    s = series if n is None else series.tail(n)
    if len(s) == 0 or line is None or np.isnan(line):
        return np.nan
    return (s > line).mean()

def format_percentage(value, decimals=1):
    try:
        val = float(value)
        if pd.isna(val):
            return "—"
        return f"{val * 100:.{decimals}f}%"
    except (ValueError, TypeError):
        return "—"

def format_ratio(value, total):
    try:
        val = float(value)
        tot = int(total)
        if pd.isna(val) or pd.isna(tot) or tot == 0:
            return "—"
        hits = int(round(val * tot))
        return f"{hits}/{tot}"
    except (ValueError, TypeError):
        return "—"

def format_moneyline(value):
    try:
        val = float(value)
        if pd.isna(val):
            return "—"
        v = int(val)
        return f"+{v}" if v > 0 else str(v)
    except (ValueError, TypeError):
        return "—"

def kelly_fraction(p, dec_odds):
    if np.isnan(p) or np.isnan(dec_odds):
        return np.nan
    b = dec_odds - 1.0
    if b <= 0:
        return np.nan
    q = 1 - p
    k = (b * p - q) / b
    return max(0.0, min(1.0, k))

def trend_pearson_r(series_last20):
    if len(series_last20) < 3:
        return np.nan
    x = np.arange(1, len(series_last20) + 1)
    return pd.Series(series_last20).corr(pd.Series(x))

def rmse_to_line(series, line, n=None):
    s = series if n is None else series.tail(n)
    if len(s) == 0 or line is None or np.isnan(line):
        return np.nan
    return math.sqrt(np.mean((s - line) ** 2))

def z_score(value, sample):
    if len(sample) < 2:
        return np.nan
    mu = sample.mean()
    sd = sample.std(ddof=1)
    if sd == 0:
        return 0.0
    return (value - mu) / sd

def series_for_player_stat(stats_df, player, stat_key):
    col = STAT_MAP.get(stat_key)
    if not col:
        return pd.DataFrame(columns=["game_date", "stat"])
    s = (
        stats_df.loc[stats_df["player_name"] == player, ["game_date", col]]
        .dropna()
        .sort_values("game_date")
        .copy()
    )
    s.rename(columns={col: "stat"}, inplace=True)
    return s

def compute_metrics_for_row(vals, line):
    """Compute row-specific metrics given a player stat series and line."""
    if vals.empty:
        return dict(
            L5=np.nan, L10=np.nan, L20=np.nan, Season=np.nan,
            hit5=np.nan, hit10=np.nan, hit20=np.nan, hit_season=np.nan,
            trend_r=np.nan, edge=np.nan, rmse10=np.nan, z_line=np.nan
        )
    season_avg = vals.mean()
    m = {
        "L5": vals.tail(5).mean(),
        "L10": vals.tail(10).mean(),
        "L20": vals.tail(20).mean(),
        "Season": season_avg,
    }
    m["hit5"] = hit_rate(vals, line, 5)
    m["hit10"] = hit_rate(vals, line, 10)
    m["hit20"] = hit_rate(vals, line, 20)
    m["hit_season"] = hit_rate(vals, line, None)
    m["trend_r"] = trend_pearson_r(vals.tail(20))
    m["edge"] = season_avg - line
    m["rmse10"] = rmse_to_line(vals, line, 10)
    m["z_line"] = z_score(line, vals.tail(20))
    return m

# ------------------------------------------------------
# CACHED LOADERS
# ------------------------------------------------------
@st.cache_data(ttl=86400, show_spinner=True)
def load_player_stats_cached(_bq_client, query):
    df = _bq_client.query(query).to_dataframe()
    # enforce numeric types
    for c in ["minutes", "pts", "reb", "ast", "stl", "blk", "pts_reb", "pts_ast", "reb_ast", "pra"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

@st.cache_data(ttl=86400, show_spinner=True)
def load_games_cached(_bq_client, query):
    return _bq_client.query(query).to_dataframe()

@st.cache_data(ttl=300, show_spinner=False)
def load_odds_bq():
    """Load odds from BigQuery (primary source) with vectorized normalization."""
    try:
        df = bq_client.query(ODDS_SQL).to_dataframe()
        df.columns = [c.lower().strip() for c in df.columns]

        # Ensure commence_time is parsed safely
        if "commence_time" in df.columns:
            df["commence_time"] = pd.to_datetime(df["commence_time"], errors="coerce")


        # numeric
        for col in ["point", "price"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # market normalization (vectorized)
        if "market" in df.columns:
            df["market_norm"] = normalize_market_series(df["market"])
        else:
            df["market_norm"] = ""

        # robust side detection (vectorized Over/Under)
        label_lower = df.get("label", "").astype(str).str.lower().fillna("")
        desc_lower = df.get("description", "").astype(str).str.lower().fillna("")

        df["side"] = ""
        df.loc[label_lower.str.contains("over") | desc_lower.str.contains("over"), "side"] = "over"
        df.loc[label_lower.str.contains("under") | desc_lower.str.contains("under"), "side"] = "under"

        st.write(
            "🟢 Sample Odds (BigQuery):",
            df[["description", "market_norm", "side", "point", "price"]].head(10),
        )
        return df
    except Exception as e:
        st.sidebar.error(f"⚠️ Could not load odds from BigQuery: {e}")
        return pd.DataFrame()

# ------------------------------------------------------
# OPTIONAL: Google Sheets odds loader (COMMENTED OUT)
# ------------------------------------------------------
"""
#@st.cache_data(ttl=300, show_spinner=False)
#def load_odds_sheet():
    # Load odds from Google Sheets (legacy). Keep for reference/fallback.
    try:
        if not SPREADSHEET_ID:
            raise ValueError("SPREADSHEET_ID is not set.")
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet(ODDS_SHEET_NAME)
        records = ws.get_all_records()
        if not records:
            st.warning("⚠️ Odds sheet is empty.")
            return pd.DataFrame()
        odds = pd.DataFrame(records)
        odds.columns = [c.lower().strip() for c in odds.columns]
        for col in ["point", "price"]:
            if col in odds.columns:
                odds[col] = pd.to_numeric(odds[col], errors="coerce")
        # Add normalized market + side
        odds["market_norm"] = normalize_market_series(odds.get("market", ""))

        desc = odds.get("description", "").astype(str).str.lower().fillna("")
        odds["side"] = ""
        odds.loc[desc.str.contains("over"), "side"] = "over"
        odds.loc[desc.str.contains("under"), "side"] = "under"

        st.write("🟡 Sample Odds (Sheets):", odds[["description", "market_norm", "side", "point", "price"]].head(10))
        return odds
    except Exception as e:
        st.sidebar.error(f"⚠️ Could not load Odds sheet: {e}")
        return pd.DataFrame()
"""

# ------------------------------------------------------
# REFRESH BUTTON + PROGRESS
# ------------------------------------------------------
refresh_clicked = st.sidebar.button("🔄 Refresh Data")
if refresh_clicked:
    st.sidebar.info("♻️ Refreshing data... please wait.")
    progress_bar = st.sidebar.progress(0)
    status_text = st.sidebar.empty()
    for pct in range(0, 101, 20):
        time.sleep(0.25)
        progress_bar.progress(pct)
        status_text.text(f"Reloading... {pct}%")
    # Clear caches and specific session keys (but not full session state)
    st.cache_data.clear()
    for key in ["player_stats", "games_df", "odds_df", "last_updated"]:
        st.session_state.pop(key, None)
    progress_bar.empty()
    status_text.text("✅ Reload complete!")
    st.rerun()

# ------------------------------------------------------
# LOAD DATA INTO SESSION (only once per session)
# ------------------------------------------------------
if "player_stats" not in st.session_state:
    with st.spinner("⏳ Loading player stats from BigQuery..."):
        st.session_state.player_stats = load_player_stats_cached(bq_client, PLAYER_STATS_SQL)
        st.session_state.last_updated = datetime.datetime.now()

if "games_df" not in st.session_state:
    with st.spinner("📅 Loading games from BigQuery..."):
        st.session_state.games_df = load_games_cached(bq_client, GAMES_SQL)
        st.session_state.last_updated = datetime.datetime.now()

if "odds_df" not in st.session_state:
    with st.spinner("📊 Loading odds data from BigQuery..."):
        st.session_state.odds_df = load_odds_bq()
        st.session_state.last_updated = datetime.datetime.now()

# Retrieve cached session data
player_stats = st.session_state.player_stats
games_df = st.session_state.games_df
odds_df = st.session_state.odds_df

# ------------------------------------------------------
# PRECOMPUTE METRICS CACHE (PLAYER x STAT) FOR SPEED
# ------------------------------------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def build_player_metric_cache(stats_df: pd.DataFrame):
    cache = {}
    if stats_df.empty:
        return cache

    # Ensure sorted by date for trend stats
    stats_df_sorted = stats_df.sort_values("game_date")

    for player, grp in stats_df_sorted.groupby("player_name"):
        cache[player] = {}
        for stat_key, col in STAT_MAP.items():
            if col not in grp.columns:
                continue
            vals = grp[col].dropna().astype(float)
            if vals.empty:
                continue
            cache[player][stat_key] = {
                "vals": vals,
                "L5": vals.tail(5).mean(),
                "L10": vals.tail(10).mean(),
                "L20": vals.tail(20).mean(),
                "Season": vals.mean(),
                "Trend": trend_pearson_r(vals.tail(20)),
            }
    return cache

metric_cache = build_player_metric_cache(player_stats)

# Show last updated
if "last_updated" in st.session_state:
    last_updated_str = st.session_state.last_updated.strftime("%Y-%m-%d %I:%M %p")
    st.sidebar.info(f"🕒 **Data last updated:** {last_updated_str}")

# ------------------------------------------------------
# DEBUG (optional)
# ------------------------------------------------------
with st.sidebar.expander("🔧 Environment Debug Info"):
    st.write(f"Project: {PROJECT_ID}")
    st.write(f"Dataset: {BIGQUERY_DATASET}")
    st.write(f"Odds Table: {ODDS_TABLE}")
    st.write(f"Sheets ID: {SPREADSHEET_ID or '—'}")

# ------------------------------------------------------
# BUILD PROPS TABLE (OPTIMIZED)
# ------------------------------------------------------
def build_props_table(
    stats_df, odds_df, games_df, date_filter,
    game_pick, player_pick, stat_pick, books,
    odds_range, min_ev, min_hit, min_kelly
):
    """Build the main Props Overview table with precomputed metrics and fast filters."""
    if games_df is None or games_df.empty:
        return pd.DataFrame()

    # Fast boolean filtering instead of .query
    g_day = games_df[games_df["game_date"] == date_filter].copy()
    if g_day.empty:
        return pd.DataFrame()

    # Optional single-game filter
    if game_pick and isinstance(game_pick, str) and " vs " in game_pick:
        home, away = game_pick.split(" vs ", 1)
        g_day = g_day[(g_day["home_team"] == home) & (g_day["visitor_team"] == away)]

    teams_today = set(g_day["home_team"]) | set(g_day["visitor_team"])

    if odds_df is None or odds_df.empty:
        return pd.DataFrame()

    o = odds_df.copy()
    o = o[o["market_norm"] != ""]

    # Book filter
    if books:
        o = o[o["bookmaker"].isin(books)]

    # Limit to teams in today's slate (if columns exist)
    if "home_team" in o.columns and "away_team" in o.columns and len(teams_today) > 0:
        o = o[o["home_team"].isin(teams_today) | o["away_team"].isin(teams_today)]

    # Stat + player filters
    if stat_pick and stat_pick != "All Stats":
        o = o[o["market_norm"] == stat_pick]
    if player_pick and player_pick != "All players":
        o = o[o["description"] == player_pick]

    # Odds range
    if "price" in o.columns:
        o["price"] = pd.to_numeric(o["price"], errors="coerce")
        o = o[o["price"].between(odds_range[0], odds_range[1])]

    if o.empty:
        return pd.DataFrame()

    rows = []
    for _, r in o.iterrows():
        player = str(r.get("description", "")).strip()
        stat_key = str(r.get("market_norm", "")).strip().lower()
        line = r.get("point", np.nan)
        book = str(r.get("bookmaker", "")).strip()
        price = r.get("price", np.nan)
        side_raw = str(r.get("side", "")).strip().lower()

        # Normalize side
        side = "over" if side_raw in ["o", "over"] else ("under" if side_raw in ["u", "under"] else "—")

        # Skip if critical info missing
        if not player or pd.isna(line) or not stat_key:
            continue

        player_metrics = metric_cache.get(player, {}).get(stat_key)
        if not player_metrics:
            continue

        vals = player_metrics["vals"]
        if vals.empty:
            continue

        # Row-specific metrics
        m = compute_metrics_for_row(vals, line)

        hit10 = m["hit10"]

        # Probability based on side
        if side == "under":
            p_hit = 1 - hit10 if not np.isnan(hit10) else np.nan
        elif side == "over":
            p_hit = hit10
        else:
            p_hit = np.nan

        # EV and Kelly
        dec = american_to_decimal(price)
        ev = np.nan if (np.isnan(p_hit) or np.isnan(dec)) else p_hit * (dec - 1) - (1 - p_hit)
        kelly = np.nan if (np.isnan(p_hit) or np.isnan(dec)) else kelly_fraction(p_hit, dec)

        rows.append({
            "Player": player,
            "Stat": STAT_LABELS.get(stat_key, stat_key),
            "Stat_key": stat_key,
            "Bookmaker": book,
            "Side": side.title() if side in ["over", "under"] else "—",
            "Line": line,
            "Price (Am)": price,
            "L5 Avg": m["L5"], "L10 Avg": m["L10"], "L20 Avg": m["L20"], "2025 Avg": m["Season"],
            "Hit5": m["hit5"], "Hit10": hit10, "Hit20": m["hit20"], "Hit Season": m["hit_season"],
            "Trend r": m["trend_r"], "Edge (Season-Line)": m["edge"], "RMSE10": m["rmse10"], "Z(Line)": m["z_line"],
            "EV": ev, "Kelly %": kelly,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Analytical filters
    if min_ev is not None:
        df = df[df["EV"] >= min_ev]
    if min_hit is not None:
        df = df[df["Hit10"] >= min_hit]
    if min_kelly is not None:
        df = df[df["Kelly %"] >= min_kelly]

    df = df.sort_values(["EV", "Hit10", "Kelly %"], ascending=[False, False, False], na_position="last").reset_index(drop=True)
    return df

@st.cache_data(ttl=120, show_spinner=False)
def get_props_table_cached(
    stats_df, odds_df, games_df, date_filter,
    game_pick, player_pick, stat_pick, books,
    odds_range, min_ev, min_hit, min_kelly
):
    return build_props_table(
        stats_df, odds_df, games_df, date_filter,
        game_pick, player_pick, stat_pick, books,
        odds_range, min_ev, min_hit, min_kelly
    )

# ------------------------------------------------------
# TREND PLOT
# ------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)
def get_player_stat_series(stats_df, player, stat_key):
    return series_for_player_stat(stats_df, player, stat_key)

def plot_trend(stats_df, player, stat_key, line, odds_df_local):
    """Plot last 20 trend with current odds display and save buttons."""
    s = get_player_stat_series(stats_df, player, stat_key)
    if s.empty:
        st.info("No stat history found.")
        return

    s = s.dropna(subset=["game_date", "stat"]).drop_duplicates(subset=["game_date"])
    s = s.sort_values("game_date").tail(20)
    season_avg = s["stat"].mean()

    # Colors for bars
    colors = np.where(s["stat"] > line, "#21c36b", "#e45757")

    # Current odds for both sides at this exact line
    price_rows = odds_df_local[
        (odds_df_local["description"].str.lower() == player.lower())
        & (odds_df_local["market_norm"] == stat_key)
        & (abs(odds_df_local["point"] - line) < 0.01)
    ].copy()

    over_price = under_price = None
    book_over = book_under = None

    if not price_rows.empty:
        over_rows = price_rows[price_rows["side"].str.lower() == "over"]
        under_rows = price_rows[price_rows["side"].str.lower() == "under"]
        if not over_rows.empty:
            over_price = int(pd.to_numeric(over_rows.iloc[0]["price"], errors="coerce"))
            book_over = over_rows.iloc[0].get("bookmaker", "")
        if not under_rows.empty:
            under_price = int(pd.to_numeric(under_rows.iloc[0]["price"], errors="coerce"))
            book_under = under_rows.iloc[0].get("bookmaker", "")

    # Centered odds display
    st.markdown("### 💰 Current Odds", unsafe_allow_html=True)
    odds_text = ""
    if over_price is not None:
        odds_text += f"**Over {line}** ({book_over}): `{format_moneyline(over_price)}`"
    if under_price is not None:
        if odds_text:
            odds_text += "  |  "
        odds_text += f"**Under {line}** ({book_under}): `{format_moneyline(under_price)}`"

    if odds_text:
        st.markdown(
            f"<div style='text-align:center; font-size:16px; margin-top:-10px; margin-bottom:12px;'>{odds_text}</div>",
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            "<div style='text-align:center; color:gray; font-size:14px;'>No recent odds found for this player/line.</div>",
            unsafe_allow_html=True
        )

    # Save Bet buttons
    c1, c2, c3 = st.columns([3, 2, 2])
    c1.write("")  # spacer
    if over_price is not None:
        if c2.button(f"⭐ Save Over {line}", key=f"save_over_{player}_{stat_key}_{line}", use_container_width=True):
            row_data = {
                "Player": player,
                "Stat": STAT_LABELS.get(stat_key, stat_key),
                "Stat_key": stat_key,
                "Bookmaker": book_over or "—",
                "Side": "Over",
                "Line": line,
                "Price (Am)": over_price,
                "Kelly %": np.nan,
                "EV": np.nan,
            }
            toggle_save(pd.Series(row_data))
    if under_price is not None:
        if c3.button(f"⭐ Save Under {line}", key=f"save_under_{player}_{stat_key}_{line}", use_container_width=True):
            row_data = {
                "Player": player,
                "Stat": STAT_LABELS.get(stat_key, stat_key),
                "Stat_key": stat_key,
                "Bookmaker": book_under or "—",
                "Side": "Under",
                "Line": line,
                "Price (Am)": under_price,
                "Kelly %": np.nan,
                "EV": np.nan,
            }
            toggle_save(pd.Series(row_data))

    # Plotly chart
    fig = go.Figure()
    fig.add_bar(
        x=s["game_date"].astype(str),
        y=s["stat"],
        name="Stat",
        marker_color=colors,
    )
    fig.add_scatter(
        x=s["game_date"].astype(str),
        y=[line] * len(s),
        name=f"Line ({line})",
        mode="lines",
        line=dict(color="#d9534f", dash="dash"),
    )
    fig.add_scatter(
        x=s["game_date"].astype(str),
        y=[season_avg] * len(s),
        name=f"Season Avg ({season_avg:.1f})",
        mode="lines",
        line=dict(color="#5cb85c"),
    )
    fig.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=80, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="center", x=0.5, font=dict(size=12)),
        xaxis=dict(title="Game Date", categoryorder="category ascending", type="category"),
        yaxis_title=STAT_LABELS.get(stat_key, stat_key).upper(),
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------
# SESSION STATE (Saved Bets)
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []  # list of dicts

def _bet_key(row):
    return f"{row['Player']}|{row['Stat_key']}|{row['Bookmaker']}|{row['Line']}|{row['Price (Am)']}"

def toggle_save(row):
    key = _bet_key(row)
    exists = any(_bet_key(x) == key for x in st.session_state.saved_bets)
    if exists:
        st.session_state.saved_bets = [x for x in st.session_state.saved_bets if _bet_key(x) != key]
        st.success("Removed from Saved Bets.")
    else:
        st.session_state.saved_bets.append(row.to_dict())
        st.success("Saved!")

# ------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙️ Filters")

# Date selector
today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)

# Game selector
if not games_df.empty and "game_date" in games_df.columns:
    day_games = games_df[games_df["game_date"] == pd.to_datetime(sel_date).date()][["home_team", "visitor_team"]].copy()
    if not day_games.empty:
        day_games["matchup"] = day_games["home_team"] + " vs " + day_games["visitor_team"]
        game_options = ["All games"] + day_games["matchup"].tolist()
    else:
        game_options = ["All games"]
else:
    game_options = ["All games"]

sel_game = st.sidebar.selectbox("Game", game_options)

# Player selector
if not player_stats.empty and "team" in player_stats.columns:
    if sel_game != "All games" and " vs " in sel_game:
        home, away = sel_game.split(" vs ", 1)
        teams = [home, away]
        mask = player_stats["team"].isin(teams)
        players_today = sorted(player_stats.loc[mask, "player_name"].unique().tolist())
    else:
        players_today = sorted(player_stats["player_name"].unique().tolist())
else:
    players_today = []

player_options = ["All players"] + players_today
sel_player = st.sidebar.selectbox("Player", player_options)

# Stat filter (raw keys)
st.sidebar.markdown("---")
st.sidebar.header("🎯 Stat Type")
stat_options = ["All Stats"] + list(STAT_MAP.keys())
default_key = "points_rebounds_assists" if "points_rebounds_assists" in STAT_MAP else stat_options[0]
try:
    default_index = stat_options.index(default_key)
except ValueError:
    default_index = 0
sel_stat_display = st.sidebar.selectbox("Stat Type (matches odds)", stat_options, index=default_index)
sel_stat = None if sel_stat_display == "All Stats" else sel_stat_display

# Table Display Options
st.sidebar.markdown("---")
st.sidebar.header("📊 Table Display Options")
show_hit_counts = st.sidebar.checkbox("Show Hit Counts (e.g. 8/10)", value=False)
all_columns = [
    "Player", "Stat", "Bookmaker", "Side", "Line", "Price (Am)",
    "L5 Avg", "L10 Avg", "L20 Avg", "2025 Avg",
    "Hit5", "Hit10", "Hit20", "Hit Season",
    "EV", "Kelly %", "Edge (Season-Line)", "Trend r"
]
default_cols = ["Player", "Stat", "Bookmaker", "Side", "Line", "Price (Am)", "EV", "Hit10", "Kelly %", "2025 Avg"]
selected_columns = st.sidebar.multiselect("Columns to Display", all_columns, default=default_cols)

# Odds filters
st.sidebar.markdown("---")
st.sidebar.header("🎲 Odds Filters")

books_available = sorted(odds_df["bookmaker"].dropna().unique().tolist()) if not odds_df.empty and "bookmaker" in odds_df.columns else []
sel_books = st.sidebar.multiselect("Bookmakers", books_available, default=books_available)

if not odds_df.empty and "price" in odds_df.columns:
    odds_float = pd.to_numeric(odds_df["price"], errors="coerce")
    odds_min = int(odds_float.min())
    odds_max = int(odds_float.max())
else:
    odds_min, odds_max = -1000, 2000

sel_odds_range = st.sidebar.slider("American odds range", odds_min, odds_max, (odds_min, odds_max))
odds_threshold = st.sidebar.number_input("Filter: Show Only Odds Above", min_value=-2000, max_value=2000, value=-600, step=50)

# Analytical filters
st.sidebar.markdown("---")
st.sidebar.header("📈 Analytical Filters")
sel_min_ev = st.sidebar.slider("Minimum EV", -1.0, 1.0, 0.0, 0.01)
sel_min_hit10 = st.sidebar.slider("Minimum Hit Rate (L10)", 0.0, 1.0, 0.5, 0.01)
sel_min_kelly = st.sidebar.slider("Minimum Kelly %", 0.0, 1.0, 0.0, 0.01)

# ------------------------------------------------------
# TABS
# ------------------------------------------------------
tab_labels = ["🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets", "📊 Prop Analytics"]
tab1, tab2, tab3, tab4 = st.tabs(tab_labels)

# ------------------------------------------------------
# TAB 1 – PROPS OVERVIEW
# ------------------------------------------------------
with tab1:
    st.subheader("Props Overview")

    df = get_props_table_cached(
        player_stats,
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
        # Sanitize numeric and apply threshold
        price_col = "Price (Am)" if "Price (Am)" in df.columns else "Price"
        df[f"{price_col}_num"] = pd.to_numeric(df[price_col], errors="coerce")
        df = df.dropna(subset=[f"{price_col}_num"])
        df = df[df[f"{price_col}_num"] >= odds_threshold]

        # Format key metrics
        df["Price (Am)"] = df[f"{price_col}_num"].apply(format_moneyline)
        df["Kelly %"] = df["Kelly %"].apply(lambda x: format_percentage(x, 1))
        df["EV"] = pd.to_numeric(df["EV"], errors="coerce").apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
        if "Edge (Season-Line)" in df.columns:
            df["Edge (Season-Line)"] = pd.to_numeric(df["Edge (Season-Line)"], errors="coerce")

        # Hit rate formatting
        for col, n in zip(["Hit5", "Hit10", "Hit20", "Hit Season"], [5, 10, 20, 20]):
            if col in df.columns:
                if show_hit_counts:
                    df[col] = df[col].apply(lambda v: f"{format_percentage(v)} ({format_ratio(v, n)})")
                else:
                    df[col] = df[col].apply(format_percentage)

        # Save Bet column
        df["Save Bet"] = False
        for i, row in df.iterrows():
            key = _bet_key(row)
            if any(_bet_key(x) == key for x in st.session_state.saved_bets):
                df.at[i, "Save Bet"] = True

        if "Stat_key" not in df.columns:
            df["Stat_key"] = ""

        # Default column order
        ordered_cols = [
            "Save Bet",
            "Player", "Stat", "Side", "Line", "Price (Am)", "Bookmaker",
            "Hit Season", "Hit20", "L20 Avg", "Hit10", "L10 Avg", "Hit5", "L5 Avg",
            "EV", "Kelly %", "Edge (Season-Line)", "Trend r"
        ]
        visible_cols = [c for c in ordered_cols if c in df.columns]
        df_display = df[visible_cols + ["Stat_key"]].copy()

        # Reset columns button (placeholder)
        st.markdown("### 📊 Player Props")
        cols_reset = st.columns([1, 6])
        with cols_reset[0]:
            if st.button("🔄 Reset Columns"):
                pass

        st.caption("💡 Drag & drop columns to reorder. Check 'Save Bet' to track favorites.")

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
            "EV": column_config.TextColumn(width="auto"),
            "Kelly %": column_config.TextColumn(width="auto"),
            "Edge (Season-Line)": column_config.NumberColumn(format="%.2f", width="auto"),
            "Trend r": column_config.NumberColumn(format="%.2f", width="auto"),
        }

        edited_df = st.data_editor(
            df_display,
            use_container_width=True,
            hide_index=True,
            key="props_editor",
            column_config=col_cfg,
        )

        # Sync Save Bet state
        for _, row in edited_df.iterrows():
            key = _bet_key(row)
            checked = row.get("Save Bet", False)
            exists = any(_bet_key(x) == key for x in st.session_state.saved_bets)
            if checked and not exists:
                st.session_state.saved_bets.append(row.to_dict())
            elif not checked and exists:
                st.session_state.saved_bets = [x for x in st.session_state.saved_bets if _bet_key(x) != key]

# ------------------------------------------------------
# TAB 2 – TREND ANALYSIS
# ------------------------------------------------------
with tab2:
    st.subheader("Trend Analysis")

    if odds_df is None or odds_df.empty:
        st.info("No odds available to analyze.")
        st.stop()

    players_in_df = ["(choose)"] + sorted(odds_df["description"].dropna().unique().tolist())
    p_pick = st.selectbox("Player", players_in_df, index=0)
    if p_pick == "(choose)":
        st.stop()

    stat_list = sorted(odds_df.loc[odds_df["description"] == p_pick, "market_norm"].dropna().unique().tolist())
    if not stat_list:
        st.warning("No available stats for this player in the odds data.")
        st.stop()
    stat_pick = st.selectbox("Stat Type (matches odds)", stat_list, index=0)

    lines = sorted(pd.to_numeric(
        odds_df.loc[(odds_df["description"] == p_pick) & (odds_df["market_norm"] == stat_pick), "point"],
        errors="coerce"
    ).dropna().unique().tolist())
    if not lines:
        st.warning("No available lines for this stat.")
        st.stop()
    line_pick = st.selectbox("Book line (threshold)", lines, index=0)

    side_pick = st.selectbox("Bet side", ["Over", "Under"], index=0)

    # Show current prices
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
            msg.append(f"**Over {line_pick}**: {int(pd.to_numeric(over_price.iloc[0], errors='coerce'))}")
        if len(under_price):
            msg.append(f"**Under {line_pick}**: {int(pd.to_numeric(under_price.iloc[0], errors='coerce'))}")
        st.markdown("💰 Current Prices: " + " | ".join(msg))
    else:
        st.markdown("_No recent odds found for this player/stat/line._")

    st.markdown(f"**Chart:** {p_pick} – {stat_pick} ({side_pick} {line_pick})")
    plot_trend(player_stats, p_pick, stat_pick, line_pick, odds_df)

# ------------------------------------------------------
# TAB 3 – SAVED BETS
# ------------------------------------------------------
with tab3:
    st.subheader("Saved Bets")

    saved_bets = st.session_state.get("saved_bets", [])
    if not saved_bets:
        st.info("No bets saved yet. Use the ⭐ Save buttons or checkbox in the Overview tab.")
    else:
        saved_df = pd.DataFrame(saved_bets)
        if "Stat_key" in saved_df.columns:
            saved_df = saved_df.drop(columns=["Stat_key"], errors="ignore")

        preferred = [
            "Player", "Stat", "Side", "Line", "Price (Am)", "Bookmaker",
            "Hit10", "L10 Avg", "Hit20", "L20 Avg", "Hit Season", "Hit5", "L5 Avg", "EV", "Kelly %"
        ]
        cols = [c for c in preferred if c in saved_df.columns] + [c for c in saved_df.columns if c not in preferred]
        st.dataframe(saved_df[cols], use_container_width=True, hide_index=True)

        csv = saved_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Saved Bets CSV", data=csv, file_name="saved_bets.csv", mime="text/csv")

# ------------------------------------------------------
# TAB 4 – PROP ANALYTICS
# ------------------------------------------------------
with tab4:
    st.subheader("Prop Analytics")

    df = get_props_table_cached(
        player_stats,
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
        # Format core data
        df["Price (Am)"] = pd.to_numeric(df.get("Price (Am)", np.nan), errors="coerce").apply(format_moneyline)
        df["EV"] = pd.to_numeric(df.get("EV", np.nan), errors="coerce").apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
        df["Edge"] = pd.to_numeric(df.get("Edge (Season-Line)", np.nan), errors="coerce").apply(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        df["Kelly %"] = df["Kelly %"].apply(lambda x: format_percentage(x, 1))

        # Add Save Bet column
        df["Save Bet"] = False
        for i, row in df.iterrows():
            key = _bet_key(row)
            if any(_bet_key(x) == key for x in st.session_state.saved_bets):
                df.at[i, "Save Bet"] = True

        # Desired layout
        ordered_cols = [
            "Save Bet", "Player", "Stat", "Side", "Line", "Price (Am)", "Bookmaker",
            "EV", "Edge", "Trend r", "Kelly %"
        ]
        visible_cols = [c for c in ordered_cols if c in df.columns]
        df_display = df[visible_cols + ["Stat_key"]] if "Stat_key" in df.columns else df[visible_cols].copy()

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
        )

        # Sync Save Bets
        for _, row in edited_df.iterrows():
            key = _bet_key(row)
            checked = row.get("Save Bet", False)
            exists = any(_bet_key(x) == key for x in st.session_state.saved_bets)
            if checked and not exists:
                st.session_state.saved_bets.append(row.to_dict())
            elif not checked and exists:
                st.session_state.saved_bets = [x for x in st.session_state.saved_bets if _bet_key(x) != key]
