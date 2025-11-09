import math
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from google.oauth2 import service_account
from google.cloud import bigquery
import gspread

# ----------------------------
# PAGE CONFIG
# ----------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# ----------------------------
# SECRETS / CONFIG
# ----------------------------
PROJECT_ID      = st.secrets.get("PROJECT_ID", "graphite-flare-477419-h7")
SPREADSHEET_ID  = st.secrets.get("SPREADSHEET_ID")            # REQUIRED
ODDS_SHEET_NAME = st.secrets.get("ODDS_SHEET_NAME", "Odds")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"], scopes=SCOPES
)
bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# ----------------------------
# SQL (union current + last season)
# ----------------------------
PLAYER_STATS_SQL = f"""
WITH stats AS (
  SELECT * FROM `{PROJECT_ID}.nba_data.player_stats`
  UNION ALL
  SELECT * FROM `{PROJECT_ID}.nba_data_2024_2025.player_stats`
)
SELECT
  player             AS player_name,
  team,
  DATE(date)         AS game_date,
  minutes, pts, reb, ast, stl, blk,
  pts+reb            AS pts_reb,
  pts+ast            AS pts_ast,
  reb+ast            AS reb_ast,
  (pts+reb+ast)      AS pra
FROM stats
"""

GAMES_SQL = f"""
WITH g AS (
  SELECT * FROM `{PROJECT_ID}.nba_data.games`
  UNION ALL
  SELECT * FROM `{PROJECT_ID}.nba_data_2024_2025.games`
)
SELECT
  game_id,
  DATE(date)   AS game_date,
  home_team,
  visitor_team,
  status
FROM g
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
    "points": "Pts","rebounds": "Reb","assists": "Ast","steals":"Stl","blocks":"Blk",
    "points_rebounds":"Pts+Reb","points_assists":"Pts+Ast","rebounds_assists":"Reb+Ast",
    "points_rebounds_assists":"PRA",
}

def american_to_decimal(odds):
    try: o = float(odds)
    except: return np.nan
    return 1 + (o/100.0) if o > 0 else 1 + (100.0/abs(o))

def hit_rate(series, line, n=None):
    s = series if n is None else series.tail(n)
    return np.nan if len(s) == 0 else (s > line).mean()

def kelly_fraction(p, dec_odds):
    if np.isnan(p) or np.isnan(dec_odds): return np.nan
    b = dec_odds - 1.0; q = 1 - p
    k = (b*p - q) / b
    return max(0.0, min(1.0, k))

def trend_pearson_r(series_last20):
    if len(series_last20) < 3: return np.nan
    x = np.arange(1, len(series_last20)+1)
    return pd.Series(series_last20).corr(pd.Series(x))

def rmse_to_line(series, line, n=None):
    s = series if n is None else series.tail(n)
    return np.nan if len(s) == 0 else math.sqrt(np.mean((s - line)**2))

def z_score(value, sample):
    if len(sample) < 2: return np.nan
    mu = sample.mean(); sd = sample.std(ddof=1)
    return 0.0 if sd == 0 else (value - mu) / sd

def normalize_market(market_str):
    if pd.isna(market_str): return ""
    m = str(market_str).lower()
    mapping = {
        "player_points_rebounds_assists": "points_rebounds_assists",
        "player_points_rebounds":        "points_rebounds",
        "player_points_assists":         "points_assists",
        "player_rebounds_assists":       "rebounds_assists",
        "player_points":                 "points",
        "player_rebounds":               "rebounds",
        "player_assists":                "assists",
        "player_steals":                 "steals",
        "player_blocks":                 "blocks",
    }
    for k, v in mapping.items():
        if k in m: return v
    return ""

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
    gc = gspread.authorize(credentials)
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(ODDS_SHEET_NAME)
    odds = pd.DataFrame(ws.get_all_records())
    odds.columns = [c.lower().strip() for c in odds.columns]
    # Expected: game_id, commence_time, in_play, bookmaker, last_update,
    # home_team, away_team, market, label, description (player), price, point
    odds["point"] = pd.to_numeric(odds.get("point", np.nan), errors="coerce")
    odds["price"] = pd.to_numeric(odds.get("price", np.nan), errors="coerce")
    if "last_update" in odds.columns:
        odds["last_update"] = pd.to_datetime(odds["last_update"], errors="coerce")
    odds["market_norm"] = odds["market"].apply(normalize_market)
    odds["bookmaker"] = odds["bookmaker"].fillna("").str.strip()
    odds["description"] = odds["description"].fillna("").str.strip()
    return odds

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

def build_props_table(stats_df, odds_df, games_df, date_filter, game_pick, player_pick, stat_pick, books, odds_range, min_ev, min_hit, min_kelly):
    # Games for selected date
    g_day = games_df.query("game_date == @date_filter").copy()
    if g_day.empty: return pd.DataFrame()

    # Optional game filter
    if game_pick and " vs " in game_pick:
        home, away = game_pick.split(" vs ", 1)
        g_day = g_day.query("home_team == @home and visitor_team == @away")

    teams_today = set(g_day["home_team"]) | set(g_day["visitor_team"])

    # Odds: keep all books, filter to teams in today's games
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
    s = series_for_player_stat(stats_df, player, stat_key)
    if s.empty:
        st.info("No stat history found."); return
    s = s.tail(20)
    season_avg = s["stat"].mean()
    colors = np.where(s["stat"] > line, "#21c36b", "#e45757")

    fig = go.Figure()
    fig.add_bar(x=s["game_date"], y=s["stat"], name="Stat", marker_color=colors)
    fig.add_scatter(x=s["game_date"], y=[line]*len(s), name="Line", mode="lines",
                    line=dict(color="#d9534f", dash="dash"))
    fig.add_scatter(x=s["game_date"], y=[season_avg]*len(s), name="Season Avg", mode="lines",
                    line=dict(color="#5cb85c"))
    fig.update_layout(height=420, margin=dict(l=20,r=20,t=40,b=20), legend=dict(orientation="h"))
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

stat_options = list(STAT_MAP.keys())
default_idx = stat_options.index("points_rebounds_assists") if "points_rebounds_assists" in stat_options else 0
sel_stat = st.sidebar.selectbox("Stat", stat_options, index=default_idx)

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
        show_cols = [c for c in df.columns if c != "Stat_key"]
        st.dataframe(df[show_cols], use_container_width=True, hide_index=True)
        st.markdown("**Click a row below to save or visualize.**")
        max_rows = min(150, len(df))
        for i in range(max_rows):
            row = df.iloc[i]
            c1, c2, c3, c4, c5 = st.columns([3,2,2,2,2])
            c1.write(f"**{row['Player']}** – {row['Stat']}  |  {row['Bookmaker']}")
            c2.write(f"**Line:** {row['Line']}")
            c3.write(f"**Price:** {int(row['Price (Am)']) if not pd.isna(row['Price (Am)']) else '—'}")
            c4.write(f"**EV:** {row['EV']:.3f}" if not pd.isna(row['EV']) else "**EV:** —")
            saved = any(_bet_key(x) == _bet_key(row) for x in st.session_state.saved_bets)
            btn_label = "⭐ Save Bet" if not saved else "❌ Remove"
            left, right = c5.columns(2)
            if left.button(btn_label, key=f"save_{i}"):
                toggle_save(row)
            if right.button("📈 Trend", key=f"trend_{i}"):
                st.session_state["trend_player"] = row["Player"]
                st.session_state["trend_stat_key"] = row["Stat_key"]
                st.session_state["trend_line"] = row["Line"]
                st.session_state["switch_to_trend"] = True
        if st.session_state.get("switch_to_trend"):
            st.session_state["switch_to_trend"] = False
            st.switch_page("app.py")  # same file; user can click Trend tab

# ----------------------------
# TAB 2 – TREND ANALYSIS
# ----------------------------
with tab2:
    st.subheader("Trend Analysis")
    # If user clicked Trend in tab1, prefill selections:
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
        stat_choices = df.loc[df["Player"]==p_pick, ["Stat","Stat_key"]].drop_duplicates()
        stat_label_list = stat_choices["Stat"].tolist()
        default_stat_label = stat_choices.loc[stat_choices["Stat_key"]==pre_k, "Stat"].iloc[0] if pre_k in stat_choices["Stat_key"].values else stat_label_list[0]
        stat_label = st.selectbox("Stat type", stat_label_list, index=stat_label_list.index(default_stat_label))
        stat_key = stat_choices.loc[stat_choices["Stat"]==stat_label, "Stat_key"].iloc[0]
        lines = sorted(df[(df["Player"]==p_pick) & (df["Stat_key"]==stat_key)]["Line"].dropna().unique().tolist())
        default_line_idx = lines.index(pre_l) if (pre_l in lines) else 0
        line_pick = st.selectbox("Book line (threshold)", lines, index=default_line_idx)
        st.markdown(f"**Chart:** {p_pick} – {stat_label} (Line {line_pick})")
        plot_trend(stats_df, p_pick, stat_key, line_pick)

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
