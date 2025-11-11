# ------------------------------------------------------
# 1️⃣ IMPORTS & CONFIG
# ------------------------------------------------------
import os, json, time, datetime, math, warnings
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

warnings.filterwarnings("ignore", category=RuntimeWarning)
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# ------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
ODDS_SHEET_NAME = os.getenv("ODDS_SHEET_NAME", "")
GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT", "")

if not PROJECT_ID or not GCP_SERVICE_ACCOUNT:
    st.error("❌ Missing environment variables — check Render settings.")
    st.stop()

# Optional quick env debug toggle
if st.sidebar.checkbox("🔍 Show env debug", value=False):
    st.sidebar.json({
        "PROJECT_ID": PROJECT_ID,
        "SPREADSHEET_ID": SPREADSHEET_ID,
        "ODDS_SHEET_NAME": ODDS_SHEET_NAME,
        "GCP_SERVICE_ACCOUNT_present": bool(GCP_SERVICE_ACCOUNT),
    })

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

# ✅ Verify connections early with clear messages
try:
    bq_client.query("SELECT 1").result()
    st.sidebar.success("✅ Connected to BigQuery")
except Exception as e:
    st.sidebar.error(f"❌ BigQuery connection failed: {e}")
    st.stop()

try:
    if SPREADSHEET_ID:
        gc.open_by_key(SPREADSHEET_ID)  # sanity access
        st.sidebar.success("✅ Connected to Google Sheets")
    else:
        st.sidebar.warning("⚠️ SPREADSHEET_ID is empty — odds will not load.")
except Exception as e:
    st.sidebar.warning(f"⚠️ Google Sheets connection failed: {e}")

# ------------------------------------------------------
# 3️⃣ CACHE SETUP
# ------------------------------------------------------
CACHE_DIR = "/data" if os.path.exists("/data") else "/tmp"
PLAYER_CACHE = f"{CACHE_DIR}/player_stats.parquet"
ODDS_CACHE = f"{CACHE_DIR}/odds_cache.json"
st.sidebar.info(f"💾 Cache dir: {CACHE_DIR}")

@st.cache_data(ttl=86400, show_spinner=True)
def load_player_stats():
    """
    Fetch player stats from BigQuery (both historical and current seasons),
    cache them for 24h, and store locally as a parquet file.
    """
    if os.path.exists(PLAYER_CACHE):
        df = pd.read_parquet(PLAYER_CACHE)
        if not df.empty:
            st.sidebar.info(f"📦 Loaded {len(df):,} player rows from cache")
            return df

    # 🔹 Combined query for both old and current season data
    query = f"""
    WITH stats AS (
        SELECT player AS player_name, team, DATE(game_date) AS game_date,
               CAST(pts AS FLOAT64) AS pts,
               CAST(reb AS FLOAT64) AS reb,
               CAST(ast AS FLOAT64) AS ast,
               CAST(stl AS FLOAT64) AS stl,
               CAST(blk AS FLOAT64) AS blk,
               CAST(pts_reb AS FLOAT64) AS pts_reb,
               CAST(pts_ast AS FLOAT64) AS pts_ast,
               CAST(reb_ast AS FLOAT64) AS reb_ast,
               CAST(pts_reb_ast AS FLOAT64) AS pra
        FROM `{PROJECT_ID}.nba_data.player_stats`
        UNION ALL
        SELECT player AS player_name, team, DATE(game_date) AS game_date,
               CAST(pts AS FLOAT64) AS pts,
               CAST(reb AS FLOAT64) AS reb,
               CAST(ast AS FLOAT64) AS ast,
               CAST(stl AS FLOAT64) AS stl,
               CAST(blk AS FLOAT64) AS blk,
               CAST(pts_reb AS FLOAT64) AS pts_reb,
               CAST(pts_ast AS FLOAT64) AS pts_ast,
               CAST(reb_ast AS FLOAT64) AS reb_ast,
               CAST(pts_reb_ast AS FLOAT64) AS pra
        FROM `{PROJECT_ID}.nba_data_2024_2025.player_stats`
    )
    SELECT * FROM stats
    """

    try:
        df = bq_client.query(query).to_dataframe()
        if df.empty:
            st.warning("⚠️ BigQuery returned no player stats — check dataset names.")
        else:
            st.sidebar.success(f"✅ Loaded {len(df):,} player rows from BigQuery")
            df.to_parquet(PLAYER_CACHE)
    except Exception as e:
        st.error(f"❌ Failed to load player stats: {e}")
        df = pd.DataFrame()

    return df

@st.cache_data(ttl=86400)
def load_games():
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
    return bq_client.query(query).to_dataframe()

def _normalize_market(s: str) -> str:
    if pd.isna(s):
        return ""
    m = str(s).lower().strip()
    mapping = {
        "player_points_rebounds_assists": "points_rebounds_assists",
        "player_points_rebounds": "points_rebounds",
        "player_points_assists": "points_assists",
        "player_rebounds_assists": "rebounds_assists",
        "player_points": "points",
        "player_rebounds": "rebounds",
        "player_assists": "assists",
        "player_steals": "steals",
        "player_blocks": "blocks",
        # common variants
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
    for k, v in mapping.items():
        if k in m:
            return v
    return ""

@st.cache_data(ttl=21600)
def load_odds_sheet():
    # Return cached JSON if present
    if os.path.exists(ODDS_CACHE):
        try:
            return pd.read_json(ODDS_CACHE)
        except Exception:
            pass

    if not SPREADSHEET_ID or not ODDS_SHEET_NAME:
        st.sidebar.warning("⚠️ SPREADSHEET_ID or ODDS_SHEET_NAME is missing.")
        return pd.DataFrame()

    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet(ODDS_SHEET_NAME)
        records = ws.get_all_records()
        if not records:
            st.warning("⚠️ Odds sheet is empty.")
            return pd.DataFrame()
        df = pd.DataFrame(records)
    except Exception as e:
        st.sidebar.warning(f"⚠️ Could not load Odds sheet: {e}")
        return pd.DataFrame()

    # Normalize columns
    df.columns = [c.lower().strip() for c in df.columns]

    # Pick a likely column for market text
    market_source_col = None
    for candidate in ["market", "market_name", "bet_type", "selection", "description"]:
        if candidate in df.columns:
            market_source_col = candidate
            break

    # market_norm
    if market_source_col:
        df["market_norm"] = df[market_source_col].apply(_normalize_market)
    else:
        df["market_norm"] = ""

    # numeric conversions
    for col in ["point", "price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # robust side extraction
    if "side" not in df.columns:
        df["side"] = ""
    # If side is empty, try infer from text-y columns
    if df["side"].eq("").all():
        import re
        possible_cols = ["label", "selection", "bet_name", "market_name", "description"]
        detected = False
        for col in possible_cols:
            if col in df.columns:
                temp = df[col].astype(str).str.lower().fillna("")
                if temp.str.contains("over").any() or temp.str.contains("under").any():
                    df["side"] = temp.apply(
                        lambda x: "over" if ("over" in x and "under" not in x)
                        else "under" if ("under" in x and "over" not in x)
                        else "over" if re.search(r"\bover\b", x)
                        else "under" if re.search(r"\bunder\b", x)
                        else ""
                    )
                    detected = True
                    break
        if not detected:
            # still nothing? leave as empty; downstream handles it
            pass

    # Guarantee important columns exist
    for col in ["bookmaker", "description"]:
        if col not in df.columns:
            df[col] = ""

    # Best-effort cache
    try:
        df.to_json(ODDS_CACHE, orient="records")
    except Exception:
        pass

    # Optional peek to help debugging
    st.write("🟢 Sample Odds:", df[["description","market_norm","side","point","price"]].head(10))
    return df

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    with st.sidebar:
        progress_bar = st.progress(0)
        status = st.empty()
        for pct in range(0, 101, 25):
            time.sleep(0.2)
            progress_bar.progress(pct)
            status.text(f"Reloading... {pct}%")
        st.cache_data.clear()
        for f in [PLAYER_CACHE, ODDS_CACHE]:
            try:
                if os.path.exists(f): os.remove(f)
            except Exception:
                pass
        progress_bar.empty()
        status.text("✅ Reload complete!")
        st.rerun()

# ------------------------------------------------------
# 4️⃣ ANALYTICS HELPERS
# ------------------------------------------------------
STAT_MAP = {
    "points": "pts", "rebounds": "reb", "assists": "ast",
    "steals": "stl", "blocks": "blk",
    "points_rebounds": "pts_reb",
    "points_assists": "pts_ast",
    "rebounds_assists": "reb_ast",
    "points_rebounds_assists": "pra",
}

STAT_LABELS = {
    "points": "Pts", "rebounds": "Reb", "assists": "Ast",
    "steals": "Stl", "blocks": "Blk",
    "points_rebounds": "Pts+Reb", "points_assists": "Pts+Ast",
    "rebounds_assists": "Reb+Ast", "points_rebounds_assists": "PRA",
}

def american_to_decimal(odds):
    try:
        o = float(odds)
    except Exception:
        return np.nan
    return 1 + (o / 100.0) if o > 0 else 1 + (100.0 / abs(o))

def hit_rate(series, line, n=None):
    s = series if n is None else series.tail(n)
    return np.nan if len(s) == 0 else (s > line).mean()

def format_percentage(value, decimals=1):
    try:
        val = float(value)
        if pd.isna(val): return "—"
        return f"{val * 100:.{decimals}f}%"
    except (ValueError, TypeError):
        return "—"

def format_ratio(value, total):
    try:
        val = float(value); tot = int(total)
        if pd.isna(val) or tot == 0: return "—"
        hits = int(round(val * tot))
        return f"{hits}/{tot}"
    except (ValueError, TypeError):
        return "—"

def format_moneyline(value):
    try:
        val = float(value)
        if pd.isna(val): return "—"
        v = int(val)
        return f"+{v}" if v > 0 else str(v)
    except (ValueError, TypeError):
        return "—"

def kelly_fraction(p, dec_odds):
    if np.isnan(p) or np.isnan(dec_odds): return np.nan
    b = dec_odds - 1.0; q = 1 - p
    k = (b * p - q) / b
    return max(0.0, min(1.0, k))

def trend_pearson_r(series_last20):
    if len(series_last20) < 3: return np.nan
    x = np.arange(1, len(series_last20) + 1)
    return pd.Series(series_last20).corr(pd.Series(x))

def rmse_to_line(series, line, n=None):
    s = series if n is None else series.tail(n)
    return np.nan if len(s) == 0 else math.sqrt(np.mean((s - line) ** 2))

def z_score(value, sample):
    if len(sample) < 2: return np.nan
    mu = sample.mean(); sd = sample.std(ddof=1)
    return 0.0 if sd == 0 else (value - mu) / sd

def series_for_player_stat(stats_df, player, stat_key):
    col = STAT_MAP.get(stat_key)
    if not col: return pd.DataFrame(columns=["game_date", "stat"])
    s = (
        stats_df.loc[stats_df["player_name"] == player, ["game_date", col]]
        .dropna().sort_values("game_date").copy()
    )
    s.rename(columns={col: "stat"}, inplace=True)
    return s

def compute_metrics_for_row(stats_df, player, stat_key, line):
    s = series_for_player_stat(stats_df, player, stat_key)
    if s.empty:
        return dict(L5=np.nan, L10=np.nan, L20=np.nan, Season=np.nan,
                    hit5=np.nan, hit10=np.nan, hit20=np.nan, hit_season=np.nan,
                    trend_r=np.nan, edge=np.nan, rmse10=np.nan, z_line=np.nan)
    vals = s["stat"].astype(float)
    season_avg = vals.mean()
    return {
        "L5": vals.tail(5).mean(),
        "L10": vals.tail(10).mean(),
        "L20": vals.tail(20).mean(),
        "Season": season_avg,
        "hit5": hit_rate(vals, line, 5),
        "hit10": hit_rate(vals, line, 10),
        "hit20": hit_rate(vals, line, 20),
        "hit_season": hit_rate(vals, line, None),
        "trend_r": trend_pearson_r(vals.tail(20)),
        "edge": season_avg - line,
        "rmse10": rmse_to_line(vals, line, 10),
        "z_line": z_score(line, vals.tail(20)),
    }

# ------------------------------------------------------
# BUILD PROPS TABLE
# ------------------------------------------------------
def build_props_table(
    stats_df, odds_df, games_df, date_filter,
    game_pick, player_pick, stat_pick, books,
    odds_range, min_ev, min_hit, min_kelly
):
    g_day = games_df.query("game_date == @date_filter").copy()
    if g_day.empty:
        return pd.DataFrame()

    if game_pick and " vs " in game_pick:
        home, away = game_pick.split(" vs ", 1)
        g_day = g_day.query("home_team == @home and visitor_team == @away")

    teams_today = set(g_day["home_team"]) | set(g_day["visitor_team"])

    o = odds_df.copy()
    if "market_norm" in o.columns:
        o = o[o["market_norm"] != ""]
    if "bookmaker" in o.columns and books:
        o = o[o["bookmaker"].isin(books)]

    if "home_team" in o.columns and "away_team" in o.columns:
        o = o[o["home_team"].isin(teams_today) | o["away_team"].isin(teams_today)]

    if stat_pick:
        o = o[o["market_norm"] == stat_pick]
    if player_pick and player_pick != "All players":
        if "description" in o.columns:
            o = o[o["description"] == player_pick]

    if "price" in o.columns and isinstance(odds_range, (list, tuple)):
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

        if side_raw in ["o", "over"]:
            side = "over"
        elif side_raw in ["u", "under"]:
            side = "under"
        else:
            side = "—"

        if not player or pd.isna(line) or not stat_key:
            continue

        m = compute_metrics_for_row(stats_df, player, stat_key, line)
        vals = series_for_player_stat(stats_df, player, stat_key)["stat"]
        if vals.empty:
            continue

        hit10 = hit_rate(vals, line, 10)
        if side == "under":
            p_hit = 1 - hit10 if not np.isnan(hit10) else np.nan
        elif side == "over":
            p_hit = hit10
        else:
            p_hit = np.nan

        dec = american_to_decimal(price)
        ev = np.nan if (np.isnan(p_hit) or np.isnan(dec)) else p_hit*(dec-1) - (1-p_hit)
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
            "Hit5": m["hit5"], "Hit10": m["hit10"], "Hit20": m["hit20"], "Hit Season": m["hit_season"],
            "Trend r": m["trend_r"], "Edge (Season-Line)": m["edge"], "RMSE10": m["rmse10"], "Z(Line)": m["z_line"],
            "EV": ev, "Kelly %": kelly,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if min_ev is not None: df = df[df["EV"] >= min_ev]
    if min_hit is not None: df = df[df["Hit10"] >= min_hit]
    if min_kelly is not None: df = df[df["Kelly %"] >= min_kelly]
    return df.sort_values(["EV", "Hit10", "Kelly %"], ascending=[False, False, False]).reset_index(drop=True)

# ------------------------------------------------------
# PLOT TREND
# ------------------------------------------------------
def plot_trend(stats_df, player, stat_key, line):
    s = series_for_player_stat(stats_df, player, stat_key)
    if s.empty:
        st.info("No stat history found.")
        return

    s = s.dropna(subset=["game_date", "stat"]).sort_values("game_date").tail(20)
    season_avg = s["stat"].mean()
    colors = np.where(s["stat"] > line, "#21c36b", "#e45757")

    fig = go.Figure()
    fig.add_bar(x=s["game_date"].astype(str), y=s["stat"], marker_color=colors, name="Stat")
    fig.add_scatter(x=s["game_date"].astype(str), y=[line]*len(s),
                    name=f"Line ({line})", mode="lines", line=dict(color="#d9534f", dash="dash"))
    fig.add_scatter(x=s["game_date"].astype(str), y=[season_avg]*len(s),
                    name=f"Season Avg ({season_avg:.1f})", mode="lines", line=dict(color="#5cb85c"))

    fig.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=60, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.08,
                    xanchor="center", x=0.5, font=dict(size=12)),
        xaxis=dict(title="Game Date", categoryorder="category ascending"),
        yaxis_title=STAT_LABELS.get(stat_key, stat_key).upper(),
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------
# 5️⃣ SESSION STATE
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

def _bet_key(row):
    return f"{row.get('Player','')}|{row.get('Stat_key','')}|{row.get('Bookmaker','')}|{row.get('Line','')}|{row.get('Price (Am)','')}"

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
# 6️⃣ LOAD DATA
# ------------------------------------------------------
with st.spinner("⏳ Loading data..."):
    player_stats = load_player_stats()
    st.sidebar.write("🧩 Columns:", list(player_stats.columns))
    st.dataframe(player_stats.head())
    games_df = load_games()
    odds_df = load_odds_sheet()

# ------------------------------------------------------
# ✅ DEBUG: Confirm data is loading properly
# ------------------------------------------------------
st.sidebar.markdown("### 🧠 Data Load Debug")

# Player stats check
st.sidebar.write("👀 Player stats shape:", player_stats.shape)

# Games table check
st.sidebar.write("🎮 Games loaded:", len(games_df))
if not games_df.empty:
    st.sidebar.write("📅 Sample game dates:", games_df["game_date"].drop_duplicates().sort_values().tail(5).tolist())
else:
    st.sidebar.warning("⚠️ games_df is empty — check your BigQuery dataset names")

# Odds sheet check
st.sidebar.write("💰 Odds rows:", len(odds_df))
st.sidebar.write("🪙 Odds columns:", list(odds_df.columns))


# ------------------------------------------------------
# 7️⃣ SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("⚙️ Filters")

today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)

if not games_df.empty and "game_date" in games_df.columns:
    day_games = games_df.query("game_date == @sel_date")[["home_team", "visitor_team"]].copy()
    if not day_games.empty:
        day_games["matchup"] = day_games["home_team"] + " vs " + day_games["visitor_team"]
        game_options = ["All games"] + day_games["matchup"].tolist()
    else:
        game_options = ["All games"]
else:
    game_options = ["All games"]
sel_game = st.sidebar.selectbox("Game", game_options)

players_today = sorted(player_stats["player_name"].unique().tolist()) if not player_stats.empty else []
sel_player = st.sidebar.selectbox("Player", ["All players"] + players_today)

stat_options = ["All Stats"] + list(STAT_MAP.keys())
sel_stat = st.sidebar.selectbox("Stat Type (matches odds sheet)", stat_options, index=0)
if sel_stat == "All Stats": sel_stat = None

st.sidebar.markdown("---")
st.sidebar.header("🎲 Odds Filters")

odds_threshold = st.sidebar.number_input("Filter: Show Only Odds Above", min_value=-2000, max_value=2000, value=-600, step=50)
books_available = sorted(odds_df["bookmaker"].dropna().unique().tolist()) if not odds_df.empty and "bookmaker" in odds_df.columns else []
sel_books = st.sidebar.multiselect("Bookmakers", books_available, default=books_available)

if not odds_df.empty and "price" in odds_df.columns:
    odds_min = int(pd.to_numeric(odds_df["price"], errors="coerce").min())
    odds_max = int(pd.to_numeric(odds_df["price"], errors="coerce").max())
else:
    odds_min, odds_max = -1000, 2000
sel_odds_range = st.sidebar.slider("American odds range", odds_min, odds_max, (odds_min, odds_max))

st.sidebar.markdown("---")
st.sidebar.header("📈 Analytical Filters")
sel_min_ev = st.sidebar.slider("Minimum EV", -1.0, 1.0, 0.0, 0.01)
sel_min_hit10 = st.sidebar.slider("Minimum Hit Rate (L10)", 0.0, 1.0, 0.5, 0.01)
sel_min_kelly = st.sidebar.slider("Minimum Kelly %", 0.0, 1.0, 0.0, 0.01)

st.sidebar.markdown("---")
show_hit_counts = st.sidebar.checkbox("Show Hit Counts (e.g. 8/10)", value=False)

st.sidebar.info("🏀 Environment ready")

# ------------------------------------------------------
# 8️⃣ MAIN UI
# ------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets", "📊 Prop Analytics"
])

# ----------------------------
# TAB 1 – PROPS OVERVIEW
# ----------------------------
with tab1:
    st.subheader("Props Overview")

    df = build_props_table(
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
        price_col = "Price (Am)" if "Price (Am)" in df.columns else "Price"
        edge_col = "Edge (Season-Line)" if "Edge (Season-Line)" in df.columns else "Edge"

        df[f"{price_col}_num"] = pd.to_numeric(df[price_col], errors="coerce")
        df = df.dropna(subset=[f"{price_col}_num"])
        df = df[df[f"{price_col}_num"] >= odds_threshold]

        df["Price (Am)"] = df[f"{price_col}_num"].apply(format_moneyline)
        df["Kelly %"] = df["Kelly %"].apply(lambda x: format_percentage(x, 1))
        df["EV"] = pd.to_numeric(df["EV"], errors="coerce").apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
        df["Edge (Season-Line)"] = pd.to_numeric(df.get(edge_col, np.nan), errors="coerce")

        for col in ["Hit5", "Hit10", "Hit20", "Hit Season"]:
            df[f"{col}_num"] = pd.to_numeric(
                df[col].astype(str).str.replace("%", "").str.replace("—", "").replace("", np.nan),
                errors="coerce"
            )

        for col, n in zip(["Hit5", "Hit10", "Hit20", "Hit Season"], [5, 10, 20, len(df)]):
            if show_hit_counts:
                df[col] = df[col].apply(lambda v: f"{format_percentage(v)} ({format_ratio(v, n)})")
            else:
                df[col] = df[col].apply(format_percentage)

        df["Save Bet"] = False
        for i, row in df.iterrows():
            key = _bet_key(row)
            if any(_bet_key(x) == key for x in st.session_state.saved_bets):
                df.at[i, "Save Bet"] = True

        if "Stat_key" not in df.columns:
            df["Stat_key"] = ""

        ordered_cols = [
            "Save Bet","Player","Stat","Side","Line","Price (Am)","Bookmaker",
            "Hit Season","Hit20","L20 Avg","Hit10","L10 Avg","Hit5","L5 Avg",
        ]

        visible_cols = [c for c in ordered_cols if c in df.columns]
        df_display = df[visible_cols + ["Stat_key"]].copy()

        if "column_order" in st.session_state:
            del st.session_state["column_order"]
        st.session_state["column_order"] = visible_cols

        st.markdown("### 📊 Player Props")
        cols_reset = st.columns([1, 6])
        with cols_reset[0]:
            if st.button("🔄 Reset Columns"):
                st.session_state["column_order"] = visible_cols

        st.caption("💡 Drag & drop columns to reorder — layout persists for your session. Check or uncheck 'Save Bet' to track favorites.")

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

        edited_df = st.data_editor(
            df_display,
            use_container_width=True,
            hide_index=True,
            key="props_editor",
            column_config=col_cfg,
            column_order=st.session_state["column_order"],
            sort_by=[f"{c}_num" for c in ["Hit Season", "Hit20", "Hit10", "Hit5"] if f"{c}_num" in df.columns],
        )

        new_order = [c for c in edited_df.columns if c != "Stat_key"]
        st.session_state["column_order"] = new_order

        for i, row in edited_df.iterrows():
            key = _bet_key(row)
            checked = row.get("Save Bet", False)
            exists = any(_bet_key(x) == key for x in st.session_state.saved_bets)
            if checked and not exists:
                st.session_state.saved_bets.append(row.to_dict())
            elif not checked and exists:
                st.session_state.saved_bets = [x for x in st.session_state.saved_bets if _bet_key(x) != key]

# ----------------------------
# TAB 2 – TREND ANALYSIS (Single Stat, Dynamic Line)
# ----------------------------
with tab2:
    st.subheader("Trend Analysis")

    if "side" not in odds_df.columns:
        odds_df["side"] = ""

    # Require that Overview produced a df; otherwise we can still allow manual choices
    try:
        df  # noqa
    except NameError:
        st.info("Open the Props Overview tab first to populate choices.")
        st.stop()
    if df is None or df.empty:
        st.info("Load props in the Overview tab first.")
        st.stop()

    pre_p = st.session_state.get("trend_player")
    pre_k = st.session_state.get("trend_stat_key")
    pre_l = st.session_state.get("trend_line")

    players_in_df = ["(choose)"] + sorted(df["Player"].unique().tolist())
    p_pick = st.selectbox(
        "Player",
        players_in_df,
        index=(players_in_df.index(pre_p) if pre_p in players_in_df else 0)
    )
    if p_pick == "(choose)":
        st.stop()

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

    lines = sorted(df[(df["Player"] == p_pick) & (df["Stat_key"] == stat_pick)]["Line"].dropna().unique().tolist())
    if not lines:
        st.warning("No available lines for this stat.")
        st.stop()

    default_line_idx = lines.index(pre_l) if (pre_l in lines) else 0
    line_pick = st.selectbox("Book line (threshold)", lines, index=default_line_idx)

    side_pick = st.selectbox("Bet side", ["Over", "Under"], index=0)

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

    st.markdown(f"**Chart:** {p_pick} – {stat_pick} ({side_pick} {line_pick})")
    # 🔧 FIX: use player_stats (not undefined stats_df)
    plot_trend(player_stats, p_pick, stat_pick, line_pick)

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

    # 🔧 FIX: pass player_stats instead of stats_df
    df = build_props_table(
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
        df["Price (Am)"] = pd.to_numeric(df.get("Price (Am)", np.nan), errors="coerce").apply(format_moneyline)
        df["EV"] = pd.to_numeric(df.get("EV", np.nan), errors="coerce").apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
        df["Edge"] = pd.to_numeric(df.get("Edge (Season-Line)", np.nan), errors="coerce").apply(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        df["Kelly %"] = df["Kelly %"].apply(lambda x: format_percentage(x, 1))

        df["Save Bet"] = False
        for i, row in df.iterrows():
            key = _bet_key(row)
            if any(_bet_key(x) == key for x in st.session_state.saved_bets):
                df.at[i, "Save Bet"] = True

        ordered_cols = [
            "Save Bet","Player","Stat","Side","Line","Price (Am)","Bookmaker",
            "EV","Edge","Trend r","Kelly %",
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

        for i, row in edited_df.iterrows():
            key = _bet_key(row)
            checked = row.get("Save Bet", False)
            exists = any(_bet_key(x) == key for x in st.session_state.saved_bets)
            if checked and not exists:
                st.session_state.saved_bets.append(row.to_dict())
            elif not checked and exists:
                st.session_state.saved_bets = [x for x in st.session_state.saved_bets if _bet_key(x) != key]
