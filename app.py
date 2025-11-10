import math
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from google.oauth2 import service_account
from google.cloud import bigquery
import gspread

# ----------------------------
# DISPLAY HELPERS (SAFE VERSIONS)
# ----------------------------
def format_percentage(value, decimals=1):
    """Safely format any numeric-like value as a percentage."""
    try:
        val = float(value)
        if pd.isna(val):
            return "—"
        return f"{val * 100:.{decimals}f}%"
    except (ValueError, TypeError):
        return "—"


def format_ratio(value, total):
    """Safely format hit ratio as 'x/y'. Accepts NaNs or bad input."""
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
    """Safely format American odds. Works even if input is string or NaN."""
    try:
        val = float(value)
        if pd.isna(val):
            return "—"
        v = int(val)
        return f"+{v}" if v > 0 else str(v)
    except (ValueError, TypeError):
        return "—"


# ----------------------------
# PAGE CONFIG
# ----------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# ----------------------------
# SECRETS / CONFIG
# ----------------------------
PROJECT_ID      = st.secrets["general"]["PROJECT_ID"]
SPREADSHEET_ID  = st.secrets["general"]["SPREADSHEET_ID"]
ODDS_SHEET_NAME = st.secrets["general"]["ODDS_SHEET_NAME"]

# ----------------------------
# GOOGLE AUTH SCOPES
# ----------------------------
SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ----------------------------
# SERVICE ACCOUNT + CLIENTS
# ----------------------------
creds = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"], scopes=SCOPES
)

# BigQuery client
try:
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    st.sidebar.success("✅ Connected to BigQuery")
except Exception as e:
    st.sidebar.error(f"❌ BigQuery connection failed: {e}")
    st.stop()

# Google Sheets client
try:
    gc = gspread.authorize(creds)
except Exception as e:
    st.sidebar.warning(f"⚠️ Could not connect to Google Sheets: {e}")
    gc = None

try:
    if gc:
        _ = gc.open_by_key(SPREADSHEET_ID)
        st.sidebar.success("✅ Connected to Google Sheets")
    else:
        raise Exception("Sheets client not initialized")
except Exception as e:
    st.sidebar.warning("⚠️ Google Sheets not connected (using empty Odds until fixed)")

# ----------------------------
# SQL – Explicit Columns, Matching Schemas
# ----------------------------

PLAYER_STATS_SQL = f"""
WITH stats AS (
  SELECT
    player AS player_name,
    team,
    DATE(game_date) AS game_date,
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
  FROM `{PROJECT_ID}.nba_data.player_stats`

  UNION ALL

  SELECT
    player AS player_name,
    team,
    DATE(game_date) AS game_date,
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
  FROM `{PROJECT_ID}.nba_data_2024_2025.player_stats`
)
SELECT *
FROM stats
"""

GAMES_SQL = f"""
WITH g AS (
  SELECT
    CAST(game_id AS INT64) AS game_id,
    DATE(date) AS game_date,
    home_team,
    visitor_team,
    CAST(home_score AS INT64) AS home_score,
    CAST(visitor_score AS INT64) AS visitor_score,
    status,
    CAST(home_team_id AS INT64) AS home_team_id,
    CAST(visitor_team_id AS INT64) AS visitor_team_id
  FROM `{PROJECT_ID}.nba_data.games`

  UNION ALL

  SELECT
    CAST(game_id AS INT64) AS game_id,
    DATE(date) AS game_date,
    home_team,
    visitor_team,
    CAST(home_score AS INT64) AS home_score,
    CAST(visitor_score AS INT64) AS visitor_score,
    status,
    CAST(home_team_id AS INT64) AS home_team_id,
    CAST(visitor_team_id AS INT64) AS visitor_team_id
  FROM `{PROJECT_ID}.nba_data_2024_2025.games`
)
SELECT *
FROM g
"""

# ----------------------------
# HELPERS (unchanged)
# ----------------------------
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
    "points": "Points",
    "rebounds": "Rebounds",
    "assists": "Assists",
    "steals": "Steals",
    "blocks": "Blocks",
    "points_rebounds": "Points+Rebounds",
    "points_assists": "Points+Assists",
    "rebounds_assists": "Rebounds+Assists",
    "points_rebounds_assists": "Pts+Reb+Ast",
}   

def american_to_decimal(odds):
    try:
        o = float(odds)
    except:
        return np.nan
    return 1 + (o / 100.0) if o > 0 else 1 + (100.0 / abs(o))

def hit_rate(series, line, n=None):
    s = series if n is None else series.tail(n)
    return np.nan if len(s) == 0 else (s > line).mean()

def kelly_fraction(p, dec_odds):
    if np.isnan(p) or np.isnan(dec_odds): 
        return np.nan
    b = dec_odds - 1.0
    q = 1 - p
    k = (b * p - q) / b
    return max(0.0, min(1.0, k))

def trend_pearson_r(series_last20):
    if len(series_last20) < 3: 
        return np.nan
    x = np.arange(1, len(series_last20)+1)
    return pd.Series(series_last20).corr(pd.Series(x))

def rmse_to_line(series, line, n=None):
    s = series if n is None else series.tail(n)
    return np.nan if len(s) == 0 else math.sqrt(np.mean((s - line)**2))

def z_score(value, sample):
    if len(sample) < 2: 
        return np.nan
    mu = sample.mean()
    sd = sample.std(ddof=1)
    return 0.0 if sd == 0 else (value - mu) / sd

def normalize_market(market_str):
    if pd.isna(market_str): return ""
    m = str(market_str).lower()
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
    }
    for k, v in mapping.items():
        if k in m:
            return v
    return ""

# ----------------------------
# LOADERS (cached)
# ----------------------------
@st.cache_data(ttl=300, show_spinner=False)
def load_player_stats():
    df = bq_client.query(PLAYER_STATS_SQL).to_dataframe()
    for c in ["minutes","pts","reb","ast","stl","blk","pts_reb","pts_ast","reb_ast","pra"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def load_games():
    return bq_client.query(GAMES_SQL).to_dataframe()

@st.cache_data(ttl=120, show_spinner=False)
def load_odds_sheet():
    """Load odds from Google Sheets with robust normalization for side and market."""
    try:
        if not SPREADSHEET_ID:
            raise ValueError("SPREADSHEET_ID is not set in secrets.toml")

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet(ODDS_SHEET_NAME)
        records = ws.get_all_records()
        if not records:
            return pd.DataFrame()

        odds = pd.DataFrame(records)
        odds.columns = [c.lower().strip() for c in odds.columns]

        # Convert price/point to numeric
        for col in ["point", "price"]:
            if col in odds.columns:
                odds[col] = pd.to_numeric(odds[col], errors="coerce")

        # --- Normalize market names ---
        def normalize_market(m):
            if pd.isna(m):
                return ""
            s = str(m).lower()
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
            }
            for k, v in mapping.items():
                if k in s:
                    return v
            return ""
        odds["market_norm"] = odds.get("market", "").apply(normalize_market)

        # --- Detect Over/Under side from any column ---
        odds["side"] = ""
        possible_cols = ["label", "selection", "market_name", "bet_name", "description"]

        for col in possible_cols:
            if col in odds.columns:
                temp = odds[col].astype(str).str.lower()
                if temp.str.contains("over").any() or temp.str.contains("under").any():
                    odds["side"] = temp.apply(
                        lambda x: "over" if "over" in x else ("under" if "under" in x else "")
                    )
                    break

        # Fallback — detect side from text like “O” / “U”
        if odds["side"].eq("").all():
            odds["side"] = odds["description"].astype(str).str.extract(r'\b(O(?:ver)?|U(?:nder)?)\b', expand=False)
            odds["side"] = odds["side"].fillna("").str.lower().replace({"o": "over", "u": "under"})

        odds["bookmaker"] = odds.get("bookmaker", "").fillna("").astype(str)
        odds["description"] = odds.get("description", "").fillna("").astype(str)

        # --- DEBUG PREVIEW ---
        st.write("🟢 Sample Odds:", odds[["description", "market_norm", "side", "point", "price"]].head(10))

        return odds

    except Exception as e:
        st.sidebar.error(f"⚠️ Could not load Odds sheet: {e}")
        return pd.DataFrame()


    except Exception as e:
        st.sidebar.error(f"⚠️ Could not load Odds sheet: {e}")
        return pd.DataFrame()





# (All other helper functions, build_props_table, plot_trend, and the three tab sections remain identical to your original.)

def series_for_player_stat(stats_df, player, stat_key):
    col = STAT_MAP.get(stat_key)
    if not col: return pd.DataFrame(columns=["game_date","stat"])
    s = stats_df.loc[stats_df["player_name"]==player, ["game_date", col]].dropna().sort_values("game_date")
    s.rename(columns={col:"stat"}, inplace=True)
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

def build_props_table(
    stats_df, odds_df, games_df, date_filter,
    game_pick, player_pick, stat_pick, books,
    odds_range, min_ev, min_hit, min_kelly
):
    """Builds the main Props Overview table with full metrics and guaranteed columns."""
def build_props_table(stats_df, odds_df, games_df, date_filter, game_pick, player_pick, stat_pick, books, odds_range, min_ev, min_hit, min_kelly):
    """Builds the main Props Overview table with full metrics and safe fallbacks."""
    g_day = games_df.query("game_date == @date_filter").copy()
    if g_day.empty: return pd.DataFrame()

    # Optional game filter
    if game_pick and " vs " in game_pick:
        home, away = game_pick.split(" vs ", 1)
        g_day = g_day.query("home_team == @home and visitor_team == @away")

    teams_today = set(g_day["home_team"]) | set(g_day["visitor_team"])

    o = odds_df.copy()
    o = o[o["market_norm"] != ""]
    o = o[o["bookmaker"].isin(books)]
    o = o[o["home_team"].isin(teams_today) | o["away_team"].isin(teams_today)]
    if stat_pick:
        o = o[o["market_norm"] == stat_pick]
    if player_pick and player_pick != "All players":
        o = o[o["description"] == player_pick]

    o = o[o["price"].between(odds_range[0], odds_range[1])]
    if o.empty: return pd.DataFrame()

    rows = []
    for _, r in o.iterrows():
        player = r["description"]; stat_key = r["market_norm"]
        line = r["point"]; book = r["bookmaker"]; price = r["price"]
        if not player or pd.isna(line) or not stat_key: continue
        m = compute_metrics_for_row(stats_df, player, stat_key, line)
        p_hit = m["hit10"] if not np.isnan(m["hit10"]) else m["hit_season"]
        dec = american_to_decimal(price)
        ev = np.nan if (np.isnan(p_hit) or np.isnan(dec)) else p_hit*(dec-1) - (1-p_hit)
        kelly = np.nan if (np.isnan(p_hit) or np.isnan(dec)) else kelly_fraction(p_hit, dec)

        rows.append({
            "Player": player,
            "Stat": STAT_LABELS.get(stat_key, stat_key),
            "Stat_key": stat_key,
            "Bookmaker": book,
            "Side": side.title() if side else "—",
            "Line": line,
            "Price (Am)": price,
            "L5 Avg": m["L5"], "L10 Avg": m["L10"], "L20 Avg": m["L20"], "2025 Avg": m["Season"],
            "Hit5": m["hit5"], "Hit10": m["hit10"], "Hit20": m["hit20"], "Hit Season": m["hit_season"],
            "Trend r": m["trend_r"], "Edge (Season-Line)": m["edge"], "RMSE10": m["rmse10"], "Z(Line)": m["z_line"],
            "EV": ev, "Kelly %": kelly,
        })

    df = pd.DataFrame(rows)
    if df.empty: return df
    if min_ev is not None:    df = df[df["EV"] >= min_ev]
    if min_hit is not None:   df = df[df["Hit10"] >= min_hit]
    if min_kelly is not None: df = df[df["Kelly %"] >= min_kelly]
    df = df.sort_values(["EV","Hit10","Kelly %"], ascending=[False, False, False], na_position="last").reset_index(drop=True)
    return df


def plot_trend(stats_df, player, stat_key, line):
    """Plot trend chart for a player's stat, skipping non-game days (no gaps)."""
    s = series_for_player_stat(stats_df, player, stat_key)
    if s.empty:
        st.info("No stat history found.")
        return

    # Filter out any empty or duplicate game dates
    s = s.dropna(subset=["game_date", "stat"]).drop_duplicates(subset=["game_date"])
    s = s.sort_values("game_date")

    # Only use the most recent 20 games (not days)
    s = s.tail(20)

    # Compute averages
    season_avg = s["stat"].mean()

    # Color bars: green if over line, red if under
    colors = np.where(s["stat"] > line, "#21c36b", "#e45757")

    # Build Plotly figure — note categoryorder fixes uneven date spacing
    fig = go.Figure()
    fig.add_bar(
        x=s["game_date"].astype(str),  # convert to string to treat as categorical
        y=s["stat"],
        name="Stat",
        marker_color=colors,
    )
    fig.add_scatter(
        x=s["game_date"].astype(str),
        y=[line] * len(s),
        name="Line",
        mode="lines",
        line=dict(color="#d9534f", dash="dash"),
    )
    fig.add_scatter(
        x=s["game_date"].astype(str),
        y=[season_avg] * len(s),
        name="Season Avg",
        mode="lines",
        line=dict(color="#5cb85c"),
    )

    fig.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h"),
        xaxis=dict(
            title="Game Date",
            categoryorder="category ascending",  # ensures chronological spacing by game
            type="category",  # categorical x-axis removes empty dates
        ),
        yaxis_title=STAT_LABELS.get(stat_key, stat_key).upper(),
    )

    st.plotly_chart(fig, use_container_width=True)


# ----------------------------
# SESSION STATE (Saved Bets)
# ----------------------------
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

# ----------------------------
# LOAD DATA
# ----------------------------
stats_df = load_player_stats()
games_df = load_games()
odds_df = load_odds_sheet()

# ----------------------------
# SIDEBAR FILTERS
# ----------------------------
st.sidebar.header("⚙️ Filters")
today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)

day_games = games_df.query("game_date == @sel_date")[["home_team","visitor_team"]].copy()
day_games["matchup"] = day_games["home_team"] + " vs " + day_games["visitor_team"]
game_options = ["All games"] + day_games["matchup"].tolist()
sel_game = st.sidebar.selectbox("Game", game_options)

if sel_game != "All games" and " vs " in sel_game:
    home, away = sel_game.split(" vs ", 1)
    teams = [home, away]
    players_today = sorted(stats_df.query("team in @teams")["player_name"].unique().tolist())
else:
    players_today = sorted(stats_df["player_name"].unique().tolist())
player_options = ["All players"] + players_today
sel_player = st.sidebar.selectbox("Player", player_options)

# ---- Stat Filter ----
stat_options = ["All Stats"] + list(STAT_MAP.keys())

# Map internal key -> pretty label
pretty_labels = ["All Stats"] + [STAT_LABELS[s] for s in STAT_MAP.keys()]

# Default to PRA if exists
default_label = "Pts+Reb+Ast" if "points_rebounds_assists" in STAT_MAP else pretty_labels[0]
sel_stat_pretty = st.sidebar.selectbox("Stat Type", pretty_labels, index=pretty_labels.index(default_label))

# Map pretty label back to internal key (or None for "All Stats")
if sel_stat_pretty == "All Stats":
    sel_stat = None
else:
    reverse_lookup = {v: k for k, v in STAT_LABELS.items()}
    sel_stat = reverse_lookup[sel_stat_pretty]
st.sidebar.markdown("---")
st.sidebar.header("📊 Table Display Options")

# Hit rate format toggle
show_hit_counts = st.sidebar.checkbox("Show Hit Counts (e.g. 8/10)", value=False)

# Column selector
all_columns = [
    "Player", "Stat", "Bookmaker", "Side", "Line", "Price (Am)",
    "L5 Avg", "L10 Avg", "L20 Avg", "2025 Avg",
    "Hit5", "Hit10", "Hit20", "Hit Season",
    "EV", "Kelly %", "Edge (Season-Line)", "Trend r"
]
selected_columns = st.sidebar.multiselect(
    "Columns to Display",
    all_columns,
    default=["Player","Stat","Bookmaker","Side","Line","Price (Am)","EV","Hit10","Kelly %","2025 Avg"]
)

# Odds filter input
st.sidebar.markdown("---")
odds_threshold = st.sidebar.number_input(
    "Filter: Show Only Odds Above",
    min_value=-2000,
    max_value=2000,
    value=-600,
    step=50
)



books_available = sorted(odds_df["bookmaker"].dropna().unique().tolist())
sel_books = st.sidebar.multiselect("Bookmakers", books_available, default=books_available)

odds_min = int(np.nanmin(odds_df["price"])) if len(odds_df) else -1000
odds_max = int(np.nanmax(odds_df["price"])) if len(odds_df) else 2000
sel_odds_range = st.sidebar.slider("American odds range", odds_min, odds_max, (-600, 600))

sel_min_ev    = st.sidebar.slider("Minimum EV", -1.0, 1.0, 0.0, 0.01)
sel_min_hit10 = st.sidebar.slider("Minimum Hit Rate (L10)", 0.0, 1.0, 0.5, 0.01)
sel_min_kelly = st.sidebar.slider("Minimum Kelly %", 0.0, 1.0, 0.0, 0.01)

# ----------------------------
# TABS
# ----------------------------
tab1, tab2, tab3 = st.tabs(["🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets"])

# ----------------------------
# TAB 1 – PROPS OVERVIEW
# ----------------------------
with tab1:
    st.subheader("Props Overview")

    df = build_props_table(
        stats_df, odds_df, games_df,
        pd.to_datetime(sel_date).date(),
        None if sel_game == "All games" else sel_game,
        None if sel_player == "All players" else sel_player,
        sel_stat, sel_books, sel_odds_range,
        sel_min_ev, sel_min_hit10, sel_min_kelly
    )

    if df.empty:
        st.info("No props match your filters.")
    else:
        # --- Ensure odds are numeric before filtering ---
        df["Price (Am)_num"] = pd.to_numeric(df["Price (Am)"], errors="coerce")

        # Drop rows with non-numeric odds
        df = df.dropna(subset=["Price (Am)_num"])

        # Apply threshold safely (integer vs float is fine here)
        df = df[df["Price (Am)_num"] >= odds_threshold]

        # From here on, use Price (Am)_num for numeric logic
        df["Price (Am)"] = df["Price (Am)_num"].apply(format_moneyline)


        # Format numeric columns
        df["Price (Am)"] = df["Price (Am)"].apply(format_moneyline)
        df["Kelly %"] = df["Kelly %"].apply(lambda x: format_percentage(x, 1))
        # --- Safe numeric formatting ---
        df["EV"] = pd.to_numeric(df["EV"], errors="coerce")
        df["2025 Avg"] = pd.to_numeric(df["2025 Avg"], errors="coerce")

        df["EV"] = df["EV"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
        df["2025 Avg"] = df["2025 Avg"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "—")


        # Format hit rates as percent or percent+ratio
        for col, n in zip(["Hit5","Hit10","Hit20","Hit Season"], [5,10,20,len(df)]):
            if show_hit_counts:
                df[col] = df[col].apply(lambda v: f"{format_percentage(v)} ({format_ratio(v,n)})")
            else:
                df[col] = df[col].apply(format_percentage)

        # Filter selected columns
        df_display = df[[c for c in selected_columns if c in df.columns]]

        # --- Gradient coloring function ---
        def color_scale(val, min_val=0, max_val=1):
            """Map percentage strings to gradient color scale (red → yellow → green)."""
            try:
                num = float(str(val).replace("%","")) / 100.0
            except:
                return ""
            ratio = (num - min_val) / (max_val - min_val)
            ratio = min(max(ratio, 0), 1)
            red = int(255 * (1 - ratio))
            green = int(255 * ratio)
            return f"background-color: rgb({red},{green},100); color: black;"

        if not df.empty:
        

            # Format numeric columns
            df["Price (Am)"] = df["Price (Am)"].apply(format_moneyline)
            df["Kelly %"] = df["Kelly %"].apply(lambda x: format_percentage(x, 1))
            # --- Safe numeric formatting ---
            df["EV"] = pd.to_numeric(df["EV"], errors="coerce")
            df["2025 Avg"] = pd.to_numeric(df["2025 Avg"], errors="coerce")

            df["EV"] = df["EV"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
            df["2025 Avg"] = df["2025 Avg"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "—")


            # Format hit rates as percent or percent+ratio
            for col, n in zip(["Hit5","Hit10","Hit20","Hit Season"], [5,10,20,len(df)]):
                if show_hit_counts:
                    df[col] = df[col].apply(lambda v: f"{format_percentage(v)} ({format_ratio(v,n)})")
                else:
                    df[col] = df[col].apply(format_percentage)

            # Filter columns
            df_display = df[[c for c in selected_columns if c in df.columns]]

            # Center align and render
            st.markdown("""
                <style>
                table td, table th {
                    text-align: center !important;
                    vertical-align: middle !important;
                }
                </style>
            """, unsafe_allow_html=True)

            st.dataframe(df_display, use_container_width=True, hide_index=True)
        else:
            with st.expander("ℹ️ Column Definitions", expanded=False):
                st.markdown("""
                **Hit5 / Hit10 / Hit20 / Hit Season**  
                → % of games in which the player went *over* the listed line  
                (e.g., Hit10 = last 10 games).  
                Toggle in the sidebar to show hit counts (e.g., 8/10).  

                **Kelly %**  
                → Suggested fraction of bankroll per the Kelly criterion based on hit rate and odds.  

                **EV**  
                → Expected Value = (Hit% × Odds) − (1 − Hit%).  

                **Edge (Season-Line)**  
                → Difference between player’s season average and the betting line.  
                """)

            st.info("No props match your filters.")

        # --- Apply styling with Pandas Styler ---
        color_cols = [c for c in ["Hit5","Hit10","Hit20","Hit Season","Kelly %"] if c in df_display.columns]
        styled_df = df_display.style \
            .map(lambda v: "text-align: center;", subset=df_display.columns) \
            .map(lambda v: "text-align: center;", subset=pd.IndexSlice[:, :])

        if color_cols:
            styled_df = styled_df.map(color_scale, subset=color_cols)


        # Display dataframe with custom styling
        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True,
        )

        # Add helpful note
        st.markdown(
            "_Tip: Green = stronger value (higher hit rate or Kelly %), red = weaker._",
            unsafe_allow_html=True
        )

        # Add smaller “Save Bet” / “Trend” cards below
        st.markdown("### 📋 Quick Actions")
        max_rows = min(100, len(df))
        for i in range(max_rows):
            row = df.iloc[i]
            c1, c2, c3, c4, c5 = st.columns([3,2,2,2,2])
            c1.write(f"**{row['Player']}** – {row['Stat']}  |  {row['Bookmaker']}")
            c2.write(f"**Line:** {row['Line']}")
            c3.write(f"**Price:** {row['Price (Am)']}")
            c4.write(f"**EV:** {row['EV']}")
            saved = any(_bet_key(x) == _bet_key(row) for x in st.session_state.saved_bets)
            btn_label = "⭐ Save Bet" if not saved else "❌ Remove"
            small_left, small_right = c5.columns(2)
            if small_left.button(btn_label, key=f"save_{i}", use_container_width=True):
                toggle_save(row)
            if small_right.button("📈 Trend", key=f"trend_{i}", use_container_width=True):
                st.session_state["trend_player"] = row["Player"]
                st.session_state["trend_stat_key"] = row["Stat_key"]
                st.session_state["trend_line"] = row["Line"]
                st.session_state["switch_to_trend"] = True

        if st.session_state.get("switch_to_trend"):
            st.session_state["switch_to_trend"] = False
            st.switch_page("app.py")

# ----------------------------
# TAB 2 – TREND ANALYSIS
# ----------------------------
with tab2:
    st.subheader("Trend Analysis")

    pre_p = st.session_state.get("trend_player")
    pre_k = st.session_state.get("trend_stat_key")
    pre_l = st.session_state.get("trend_line")

    if 'df' not in locals() or df is None or df.empty:
        st.info("Load props in the Overview tab first.")
    else:
        players_in_df = ["(choose)"] + sorted(df["Player"].unique().tolist())
        p_pick = st.selectbox("Player", players_in_df, index=(players_in_df.index(pre_p) if pre_p in players_in_df else 0))
        if p_pick == "(choose)":
            st.stop()

        # Available stat options
        stat_choices = df.loc[df["Player"]==p_pick, ["Stat","Stat_key"]].drop_duplicates()
        stat_label_list = stat_choices["Stat"].tolist()
        default_stat_label = (
            stat_choices.loc[stat_choices["Stat_key"]==pre_k, "Stat"].iloc[0]
            if pre_k in stat_choices["Stat_key"].values
            else stat_label_list[0]
        )
        stat_label = st.selectbox("Stat type", stat_label_list, index=stat_label_list.index(default_stat_label))
        stat_key = stat_choices.loc[stat_choices["Stat"]==stat_label, "Stat_key"].iloc[0]

        # Line choices
        lines = sorted(df[(df["Player"]==p_pick) & (df["Stat_key"]==stat_key)]["Line"].dropna().unique().tolist())
        default_line_idx = lines.index(pre_l) if (pre_l in lines) else 0
        line_pick = st.selectbox("Book line (threshold)", lines, index=default_line_idx)

        # NEW: Over / Under toggle
        side_pick = st.selectbox("Bet side", ["Over", "Under"])

        # --- Optional price snippet ---
        price_rows = odds_df[
            (odds_df["description"] == p_pick)
            & (odds_df["market_norm"] == stat_key)
            & (abs(odds_df["point"] - line_pick) < 0.01)
        ]
        if not price_rows.empty:
            over_price = price_rows.loc[price_rows["side"] == "over", "price"].dropna()
            under_price = price_rows.loc[price_rows["side"] == "under", "price"].dropna()
            msg = []
            if len(over_price):
                msg.append(f"**Over {line_pick}**: {int(over_price.iloc[0])}")
            if len(under_price):
                msg.append(f"**Under {line_pick}**: {int(under_price.iloc[0])}")
            st.markdown("💰 Current Prices: " + " | ".join(msg))
        else:
            st.markdown("_No recent prices found for this player/line._")

        # --- Build chart ---
        st.markdown(f"**Chart:** {p_pick} – {stat_label} ({side_pick} {line_pick})")

        s = series_for_player_stat(stats_df, p_pick, stat_key)
        if s.empty:
            st.info("No stat history found.")
            st.stop()

        s = s.dropna(subset=["game_date","stat"]).drop_duplicates(subset=["game_date"]).sort_values("game_date")
        s = s.tail(20)
        season_avg = s["stat"].mean()

        # Color bars depending on selected side
        if side_pick.lower() == "over":
            colors = np.where(s["stat"] > line_pick, "#21c36b", "#e0e0e0")  # green for hits
        else:
            colors = np.where(s["stat"] < line_pick, "#e45757", "#e0e0e0")  # red for hits

        # Plot
        fig = go.Figure()
        fig.add_bar(x=s["game_date"].astype(str), y=s["stat"], name="Stat", marker_color=colors)
        fig.add_scatter(
            x=s["game_date"].astype(str),
            y=[line_pick]*len(s),
            name="Line",
            mode="lines",
            line=dict(color="#d9534f", dash="dash"),
        )
        fig.add_scatter(
            x=s["game_date"].astype(str),
            y=[season_avg]*len(s),
            name="Season Avg",
            mode="lines",
            line=dict(color="#5cb85c"),
        )
        fig.update_layout(
            height=420,
            margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(orientation="h"),
            xaxis=dict(categoryorder="category ascending", type="category", title="Game Date"),
            yaxis_title=stat_label.upper(),
        )
        st.plotly_chart(fig, use_container_width=True)


# ----------------------------
# TAB 3 – SAVED BETS (PER-VIEWER)
# ----------------------------
with tab3:
    st.subheader("Saved Bets (private to you)")
    if len(st.session_state.saved_bets) == 0:
        st.info("No bets saved yet. Use ⭐ Save Bet in the Overview tab.")
    else:
        saved_df = pd.DataFrame(st.session_state.saved_bets)
        preferred = ["Player","Stat","Bookmaker","Line","Price (Am)","EV","Kelly %","Hit10","L10 Avg","2025 Avg","Edge (Season-Line)"]
        cols = [c for c in preferred if c in saved_df.columns] + [c for c in saved_df.columns if c not in preferred]
        st.dataframe(saved_df[cols], use_container_width=True, hide_index=True)
        csv = saved_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download CSV", data=csv, file_name="saved_bets.csv", mime="text/csv")