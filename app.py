import os
import math
import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from google.oauth2 import service_account
from google.cloud import bigquery
import gspread
import streamlit as st  # 👈 keep this import here

# ------------------------------------------------------
# MUST BE FIRST STREAMLIT COMMAND
# ------------------------------------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# ------------------------------------------------------
# ENVIRONMENT VARIABLES (from Render)
# ------------------------------------------------------
PROJECT_ID = os.environ["PROJECT_ID"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
ODDS_SHEET_NAME = os.environ["ODDS_SHEET_NAME"]

# Load GCP credentials (as JSON string)
creds_dict = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
credentials = service_account.Credentials.from_service_account_info(creds_dict)

# ------------------------------------------------------
# REST OF YOUR APP BELOW
# ------------------------------------------------------
st.title("NBA Prop Analyzer 🏀")

# Example: check that credentials and env vars loaded
st.write("✅ Environment loaded successfully!")



# ----------------------------
# PAGE CONFIG
# ----------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# ----------------------------
# SECRETS / CONFIG
# ----------------------------
PROJECT_ID = st.secrets["general"]["PROJECT_ID"]
SPREADSHEET_ID = st.secrets["general"]["SPREADSHEET_ID"]
ODDS_SHEET_NAME = st.secrets["general"]["ODDS_SHEET_NAME"]

# ---- SCOPES ----
SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ---- SERVICE ACCOUNT ----
creds = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=SCOPES,
)

# ---- CLIENTS ----
try:
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    st.sidebar.success("✅ Connected to BigQuery")
except Exception as e:
    st.sidebar.error(f"❌ BigQuery connection failed: {e}")
    st.stop()

try:
    gc = gspread.authorize(creds)
except Exception as e:
    st.sidebar.warning(f"⚠️ Could not connect to Google Sheets: {e}")

try:
    _ = gc.open_by_key(SPREADSHEET_ID)  # probe access
    st.sidebar.success("✅ Connected to Google Sheets")
except Exception as e:
    st.sidebar.warning("⚠️ Google Sheets not connected (using empty Odds until fixed)")

# ----------------------------
# SQL (UNION across seasons; force compatible types)
# ----------------------------
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
  FROM {PROJECT_ID}.nba_data.player_stats
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
  FROM {PROJECT_ID}.nba_data_2024_2025.player_stats
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
  FROM {PROJECT_ID}.nba_data.games
  UNION ALL
  SELECT
    CAST(game_id AS STRING) AS game_id,
    CAST(DATE(date) AS DATE) AS game_date,
    home_team,
    visitor_team,
    status
  FROM {PROJECT_ID}.nba_data_2024_2025.games
)
SELECT * FROM g
"""

# ----------------------------
# HELPERS
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
    x = np.arange(1, len(series_last20) + 1)
    return pd.Series(series_last20).corr(pd.Series(x))

def rmse_to_line(series, line, n=None):
    s = series if n is None else series.tail(n)
    return np.nan if len(s) == 0 else math.sqrt(np.mean((s - line) ** 2))

def z_score(value, sample):
    if len(sample) < 2:
        return np.nan
    mu = sample.mean()
    sd = sample.std(ddof=1)
    return 0.0 if sd == 0 else (value - mu) / sd

def normalize_market(market_str):
    if pd.isna(market_str):
        return ""
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

    import warnings

    # Suppress harmless runtime warnings (NumPy correlation/variance issues)
    warnings.filterwarnings("ignore", category=RuntimeWarning)

# ----------------------------
# LOADERS (cached)
# ----------------------------
@st.cache_data(ttl=300, show_spinner=False)
def load_player_stats():
    df = bq_client.query(PLAYER_STATS_SQL).to_dataframe()
    for c in ["minutes", "pts", "reb", "ast", "stl", "blk", "pts_reb", "pts_ast", "reb_ast", "pra"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def load_games():
    return bq_client.query(GAMES_SQL).to_dataframe()

@st.cache_data(ttl=120, show_spinner=False)
def load_odds_sheet():
    """Load odds from Google Sheets, normalizing market + side columns robustly."""
    try:
        if not SPREADSHEET_ID:
            raise ValueError("SPREADSHEET_ID is not set in secrets.toml")

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet(ODDS_SHEET_NAME)
        records = ws.get_all_records()

        if not records:
            st.warning("⚠️ Odds sheet is empty.")
            return pd.DataFrame()

        # Convert to DataFrame + clean columns
        odds = pd.DataFrame(records)
        odds.columns = [c.lower().strip() for c in odds.columns]

        # --- Convert numeric columns safely ---
        for col in ["point", "price"]:
            if col in odds.columns:
                odds[col] = pd.to_numeric(odds[col], errors="coerce")

        # --- Normalize market column to match your stat keys (no pretty renaming) ---
        def normalize_market(m):
            if pd.isna(m):
                return ""
            s = str(m).lower().strip()
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
            for k, v in mapping.items():
                if k in s:
                    return v
            return ""

        # Choose best column for market source
        market_source_col = None
        for candidate in ["market", "market_name", "bet_type", "selection", "description"]:
            if candidate in odds.columns:
                market_source_col = candidate
                break

        if not market_source_col:
            raise ValueError("No valid market column found in odds sheet.")

        odds["market_norm"] = odds[market_source_col].apply(normalize_market)

        # --- Robust Over/Under detection ---
        odds["side"] = ""
        possible_cols = ["label", "selection", "bet_name", "market_name", "description"]

        import re
        detected = False
        for col in possible_cols:
            if col in odds.columns:
                temp = odds[col].astype(str).str.lower().fillna("")
                if temp.str.contains("over").any() or temp.str.contains("under").any():
                    odds["side"] = temp.apply(
                        lambda x: (
                            "over" if "over" in x and "under" not in x
                            else "under" if "under" in x and "over" not in x
                            else "under" if re.search(r"\bunder\b", x)
                            else "over" if re.search(r"\bover\b", x)
                            else ""
                        )
                    )
                    detected = True
                    break

        # Fallback: regex shorthand O / U
        if not detected or odds["side"].eq("").all():
            desc = odds["description"].astype(str).str.lower().fillna("")
            odds["side"] = desc.apply(
                lambda x: (
                    "over" if re.search(r"\b(o|over)\b", x)
                    else "under" if re.search(r"\b(u|under)\b", x)
                    else ""
                )
            )

        # Normalize and clean
        odds["side"] = odds["side"].str.strip().str.lower().replace({"o": "over", "u": "under"})

        # --- Ensure all expected columns exist ---
        for col in ["bookmaker", "description", "market_norm", "side"]:
            if col not in odds.columns:
                odds[col] = ""
            odds[col] = odds[col].fillna("").astype(str)

        # --- Debug preview ---
        st.write("🟢 Sample Odds:", odds[["description", "market_norm", "side", "point", "price"]].head(10))

        # --- Check for unknown markets ---
        known_stats = set(STAT_MAP.keys())
        found_markets = set(odds["market_norm"].dropna().unique())
        unknown_markets = found_markets - known_stats
        if unknown_markets:
            st.warning(
                f"⚠️ Unknown stat markets found in odds sheet: {unknown_markets}. "
                "These may not match your player stats columns and could cause blank hit rates."
            )

        # --- Guarantee side column exists ---
        if "side" not in odds.columns:
            odds["side"] = ""

        return odds

    except Exception as e:
        st.sidebar.error(f"⚠️ Could not load Odds sheet: {e}")
        return pd.DataFrame()

# ----------------------------
# (Your original analysis helpers)
# ----------------------------
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

def compute_metrics_for_row(stats_df, player, stat_key, line):
    s = series_for_player_stat(stats_df, player, stat_key)
    if s.empty:
        return dict(
            L5=np.nan, L10=np.nan, L20=np.nan, Season=np.nan,
            hit5=np.nan, hit10=np.nan, hit20=np.nan, hit_season=np.nan,
            trend_r=np.nan, edge=np.nan, rmse10=np.nan, z_line=np.nan
        )

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

    # --- Filter odds by markets and teams ---
    o = odds_df.copy()
    o = o[o["market_norm"] != ""]
    o = o[o["bookmaker"].isin(books)]

    if "home_team" in o.columns and "away_team" in o.columns:
        o = o[o["home_team"].isin(teams_today) | o["away_team"].isin(teams_today)]

    if stat_pick and stat_pick != "All Stats":
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
        player = str(r.get("description", "")).strip()
        stat_key = str(r.get("market_norm", "")).strip().lower()
        line = r.get("point", np.nan)
        book = str(r.get("bookmaker", "")).strip()
        price = r.get("price", np.nan)
        side_raw = str(r.get("side", "")).strip().lower()

        # Normalize side values
        if side_raw in ["o", "over"]:
            side = "over"
        elif side_raw in ["u", "under"]:
            side = "under"
        else:
            side = "—"

        # Skip if critical info is missing
        if not player or pd.isna(line) or not stat_key:
            continue

        # --- Compute metrics for player/stat ---
        m = compute_metrics_for_row(stats_df, player, stat_key, line)
        vals = series_for_player_stat(stats_df, player, stat_key)["stat"]
        if vals.empty:
            continue

        # --- Compute hit rates ---
        hit5 = hit_rate(vals, line, 5)
        hit10 = hit_rate(vals, line, 10)
        hit20 = hit_rate(vals, line, 20)
        hit_season = hit_rate(vals, line, None)

        # --- Compute probability based on side ---
        if side == "under":
            p_hit = 1 - hit10 if not np.isnan(hit10) else np.nan
        elif side == "over":
            p_hit = hit10
        else:
            p_hit = np.nan

        # --- Calculate EV and Kelly % ---
        dec = american_to_decimal(price)
        ev = np.nan if (np.isnan(p_hit) or np.isnan(dec)) else p_hit*(dec-1) - (1-p_hit)
        kelly = np.nan if (np.isnan(p_hit) or np.isnan(dec)) else kelly_fraction(p_hit, dec)

        # --- Add to rows ---
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

    # --- Combine rows into DataFrame ---
    df = pd.DataFrame(rows)
    if df.empty: return df
    if min_ev is not None:    df = df[df["EV"] >= min_ev]
    if min_hit is not None:   df = df[df["Hit10"] >= min_hit]
    if min_kelly is not None: df = df[df["Kelly %"] >= min_kelly]
    df = df.sort_values(["EV","Hit10","Kelly %"], ascending=[False, False, False], na_position="last").reset_index(drop=True)
    return df


def plot_trend(stats_df, player, stat_key, line):
    """Plot trend chart for a player's stat, with odds and Save Bet buttons centered above."""
    s = series_for_player_stat(stats_df, player, stat_key)
    if s.empty:
        st.info("No stat history found.")
        return

    s = s.dropna(subset=["game_date", "stat"]).drop_duplicates(subset=["game_date"])
    s = s.sort_values("game_date").tail(20)
    season_avg = s["stat"].mean()

    # --- Colors for bars: green = hit, red = miss ---
    colors = np.where(s["stat"] > line, "#21c36b", "#e45757")

    # --- Fetch current odds for both Over and Under ---
    price_rows = odds_df[
        (odds_df["description"] == player)
        & (odds_df["market_norm"] == stat_key)
        & (abs(odds_df["point"] - line) < 0.01)
    ]

    over_price = under_price = None
    book_over = book_under = None

    if not price_rows.empty:
        over_rows = price_rows[price_rows["side"].str.lower() == "over"]
        under_rows = price_rows[price_rows["side"].str.lower() == "under"]
        if not over_rows.empty:
            over_price = int(over_rows.iloc[0]["price"])
            book_over = over_rows.iloc[0].get("bookmaker", "")
        if not under_rows.empty:
            under_price = int(under_rows.iloc[0]["price"])
            book_under = under_rows.iloc[0].get("bookmaker", "")

    # --- Centered odds display above chart ---
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

    # --- Add Save Bet buttons side by side ---
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

    # --- Plotly chart ---
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

    # --- Legend above chart ---
    fig.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=80, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.08,
            xanchor="center",
            x=0.5,
            font=dict(size=12)
        ),
        xaxis=dict(
            title="Game Date",
            categoryorder="category ascending",
            type="category",
        ),
        yaxis_title=STAT_LABELS.get(stat_key, stat_key).upper(),
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- Display current odds for this line ---
    odds_row = odds_df[
        (odds_df["description"].str.lower() == player.lower())
        & (odds_df["market_norm"] == stat_key)
        & (abs(odds_df["point"] - line) < 0.01)
    ]

    if not odds_row.empty:
        over_price = odds_row["price"].iloc[0]
        under_price = odds_row["price"].iloc[-1] if len(odds_row) > 1 else None
        bookmaker = odds_row["bookmaker"].iloc[0]
        st.markdown(
            f"**Current Odds:** {bookmaker} — Over {over_price:+}, Under {under_price or 'N/A'}"
        )
    else:
        st.caption("No odds found for this line.")


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
# SIDEBAR FILTERS (RAW STAT KEYS, ORIGINAL FORMATTING)
# ----------------------------
st.sidebar.header("⚙️ Filters")

# --- Date Selector ---
today = pd.Timestamp.today().normalize()
sel_date = st.sidebar.date_input("Game date", today)

# --- Game Selector ---
if not games_df.empty and "game_date" in games_df.columns:
    day_games = games_df.query("game_date == @sel_date")[["home_team", "visitor_team"]].copy()
    day_games["matchup"] = day_games["home_team"] + " vs " + day_games["visitor_team"]
    game_options = ["All games"] + day_games["matchup"].tolist()
else:
    game_options = ["All games"]

sel_game = st.sidebar.selectbox("Game", game_options)

# --- Player Selector ---
if not stats_df.empty and "team" in stats_df.columns:
    if sel_game != "All games" and " vs " in sel_game:
        home, away = sel_game.split(" vs ", 1)
        teams = [home, away]
        players_today = sorted(stats_df.query("team in @teams")["player_name"].unique().tolist())
    else:
        players_today = sorted(stats_df["player_name"].unique().tolist())
else:
    players_today = []

player_options = ["All players"] + players_today
sel_player = st.sidebar.selectbox("Player", player_options)

# --- Stat Filter (Raw keys only, matches odds sheet exactly) ---
st.sidebar.markdown("---")
st.sidebar.header("🎯 Stat Type")

stat_options = ["All Stats"] + list(STAT_MAP.keys())

# Default to "points_rebounds_assists" if it exists; otherwise use the first option
default_key = "points_rebounds_assists" if "points_rebounds_assists" in STAT_MAP else stat_options[0]
try:
    default_index = stat_options.index(default_key)
except ValueError:
    default_index = 0

sel_stat = st.sidebar.selectbox("Stat Type (matches odds sheet)", stat_options, index=default_index)

# Convert "All Stats" to None so build_props_table() includes all props
if sel_stat == "All Stats":
    sel_stat = None

# --- Table Display Options ---
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

selected_columns = st.sidebar.multiselect(
    "Columns to Display",
    all_columns,
    default=default_cols
)

# --- Odds Filter Inputs ---
st.sidebar.markdown("---")
st.sidebar.header("🎲 Odds Filters")

odds_threshold = st.sidebar.number_input(
    "Filter: Show Only Odds Above",
    min_value=-2000,
    max_value=2000,
    value=-600,
    step=50
)

books_available = sorted(odds_df["bookmaker"].dropna().unique().tolist()) if not odds_df.empty else []
sel_books = st.sidebar.multiselect("Bookmakers", books_available, default=books_available)

if not odds_df.empty:
    odds_min = int(pd.to_numeric(odds_df["price"], errors="coerce").min())
    odds_max = int(pd.to_numeric(odds_df["price"], errors="coerce").max())
else:
    odds_min, odds_max = -1000, 2000

sel_odds_range = st.sidebar.slider("American odds range", odds_min, odds_max, (odds_min, odds_max))

# --- Analytical Filters ---
st.sidebar.markdown("---")
st.sidebar.header("📈 Analytical Filters")

sel_min_ev = st.sidebar.slider("Minimum EV", -1.0, 1.0, 0.0, 0.01)
sel_min_hit10 = st.sidebar.slider("Minimum Hit Rate (L10)", 0.0, 1.0, 0.5, 0.01)
sel_min_kelly = st.sidebar.slider("Minimum Kelly %", 0.0, 1.0, 0.0, 0.01)


# --- Debug info (optional) ---
# st.sidebar.write("DEBUG", sel_stat, sel_game, sel_player)


# ----------------------------
# TABS
# ----------------------------
tab_labels = ["🧮 Props Overview", "📈 Trend Analysis", "📋 Saved Bets", "📊 Prop Analytics"]
tab1, tab2, tab3, tab4 = st.tabs(tab_labels)

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

        # Hit rate formatting
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

        # --- Force refresh of layout on schema change ---
        if "column_order" in st.session_state:
            del st.session_state["column_order"]
        st.session_state["column_order"] = visible_cols

        # --- Display title + reset ---
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
        )

        # --- Save column order ---
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
                st.session_state.saved_bets = [x for x in st.session_state.saved_bets if _bet_key(x) != key]

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
