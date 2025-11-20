# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
from datetime import datetime
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytz
import requests
import streamlit as st
import psycopg2
import psycopg2.extras
import jwt

from google.cloud import bigquery
from google.oauth2 import service_account

# ------------------------------------------------------
# TIMEZONE (EST)
# ------------------------------------------------------
EST = pytz.timezone("America/New_York")

# ------------------------------------------------------
# STREAMLIT CONFIG
# ------------------------------------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# Global CSS
st.markdown(
    """
    <style>
    table td, table th {
        text-align: center !important;
        vertical-align: middle !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")

# Let dataset be configurable but default to your existing value
DATASET = os.getenv("BIGQUERY_DATASET", "nba_prop_analyzer")
PROPS_TABLE = "todays_props_with_hit_rates"
HISTORICAL_TABLE = "historical_player_stats_for_trends"

SERVICE_JSON = os.getenv("GCP_SERVICE_ACCOUNT", "")

# Auth0
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN", "")
AUTH0_CLIENT_ID = os.getenv("AUTH0_CLIENT_ID", "")
AUTH0_CLIENT_SECRET = os.getenv("AUTH0_CLIENT_SECRET", "")
AUTH0_REDIRECT_URI = os.getenv("AUTH0_REDIRECT_URI", "")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE", "")

# Render PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


missing_env = []
if not PROJECT_ID:
    missing_env.append("PROJECT_ID")
if not SERVICE_JSON:
    missing_env.append("GCP_SERVICE_ACCOUNT")
if not DATABASE_URL:
    missing_env.append("DATABASE_URL")
if not AUTH0_DOMAIN:
    missing_env.append("AUTH0_DOMAIN")
if not AUTH0_CLIENT_ID:
    missing_env.append("AUTH0_CLIENT_ID")
if not AUTH0_CLIENT_SECRET:
    missing_env.append("AUTH0_CLIENT_SECRET")
if not AUTH0_REDIRECT_URI:
    missing_env.append("AUTH0_REDIRECT_URI")
if not AUTH0_AUDIENCE:
    missing_env.append("AUTH0_AUDIENCE")

if missing_env:
    st.error(
        "❌ Missing required environment variables:\n\n"
        + "\n".join(f"- {m}" for m in missing_env)
    )
    st.stop()

# ------------------------------------------------------
# SQL STATEMENTS (BIGQUERY)
# ------------------------------------------------------
PROPS_SQL = f"""
SELECT *
FROM `{PROJECT_ID}.{DATASET}.{PROPS_TABLE}`
"""

HISTORICAL_SQL = f"""
SELECT
  player,
  player_team,
  home_team,
  visitor_team,
  game_date,
  opponent_team,
  home_away,
  pts,
  reb,
  ast,
  pra
FROM `{PROJECT_ID}.{DATASET}.{HISTORICAL_TABLE}`
ORDER BY game_date
"""

# ------------------------------------------------------
# AUTHENTICATION – GOOGLE BIGQUERY
# ------------------------------------------------------
try:
    creds_dict = json.loads(SERVICE_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery",
        ],
    )
except Exception as e:
    st.error(f"❌ BigQuery credential error: {e}")
    st.stop()

# ------------------------------------------------------
# BIGQUERY CLIENT
# ------------------------------------------------------
try:
    bq_client = bigquery.Client(credentials=creds, project=PROJECT_ID)
    _ = bq_client.query("SELECT 1").result()
    st.sidebar.success("Connected to BigQuery")
except Exception as e:
    st.sidebar.error(f"BigQuery connection failed: {e}")
    st.stop()

# ------------------------------------------------------
# RENDER POSTGRES CONNECTION HELPERS
# ------------------------------------------------------
def get_db_conn():
    """
    Create a new PostgreSQL connection to your Render database.

    Render URLs usually already include sslmode=require, but we enforce it
    as a keyword arg too just to be safe.
    """
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db_schema():
    """
    Create tables if they don't exist.
    Safe to run on every startup.
    """
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                auth0_sub TEXT UNIQUE NOT NULL,
                email TEXT
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_bets (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                bet_name TEXT,
                bet_details JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )

        conn.commit()
        conn.close()
    except Exception as e:
        st.sidebar.error(f"DB init error: {e}")


init_db_schema()


def get_or_create_user(auth0_sub: str, email: str):
    """
    Ensure a user exists in the 'users' table and return the row.
    """
    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT * FROM users WHERE auth0_sub = %s", (auth0_sub,))
    row = cur.fetchone()

    if row:
        conn.close()
        return row

    cur.execute(
        "INSERT INTO users (auth0_sub, email) VALUES (%s, %s) RETURNING *",
        (auth0_sub, email),
    )
    row = cur.fetchone()
    conn.commit()
    conn.close()
    return row


def load_saved_bets_from_db(user_id: int):
    """
    Load saved bets from DB as a list of dicts.
    """
    try:
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT bet_details FROM saved_bets WHERE user_id = %s ORDER BY created_at DESC",
            (user_id,),
        )
        rows = cur.fetchall()
        conn.close()

        bets = []
        for r in rows:
            details = r.get("bet_details")
            if isinstance(details, dict):
                bets.append(details)
        return bets
    except Exception as e:
        st.sidebar.warning(f"Could not load saved bets from DB: {e}")
        return []


def replace_saved_bets_in_db(user_id: int, bets: list[dict]):
    """
    Replace all saved bets for this user with the current list in memory.
    Simple: DELETE then INSERT.
    """
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        cur.execute("DELETE FROM saved_bets WHERE user_id = %s", (user_id,))

        for bet in bets:
            bet_name = (
                f"{bet.get('player', '')} "
                f"{bet.get('market', '')} "
                f"{bet.get('line', '')} "
                f"{bet.get('bet_type', '')}"
            ).strip() or "Bet"

            cur.execute(
                """
                INSERT INTO saved_bets (user_id, bet_name, bet_details)
                VALUES (%s, %s, %s)
                """,
                (user_id, bet_name, psycopg2.extras.Json(bet)),
            )

        conn.commit()
        conn.close()
    except Exception as e:
        st.sidebar.error(f"Error saving bets to DB: {e}")

# ------------------------------------------------------
# AUTH0 HELPERS (LOGIN)
# ------------------------------------------------------
def get_auth0_authorize_url():
    params = {
        "response_type": "code",
        "client_id": AUTH0_CLIENT_ID,
        "redirect_uri": AUTH0_REDIRECT_URI,
        "scope": "openid profile email",
        "audience": AUTH0_AUDIENCE,
    }
    return f"https://{AUTH0_DOMAIN}/authorize?{urlencode(params)}"


def exchange_code_for_token(code: str):
    token_url = f"https://{AUTH0_DOMAIN}/oauth/token"
    payload = {
        "grant_type": "authorization_code",
        "client_id": AUTH0_CLIENT_ID,
        "client_secret": AUTH0_CLIENT_SECRET,
        "code": code,
        "redirect_uri": AUTH0_REDIRECT_URI,
        "audience": AUTH0_AUDIENCE,
    }
    resp = requests.post(token_url, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def decode_id_token(id_token: str):
    """
    For simplicity, we disable signature & audience verification here.
    For production you should verify the token using Auth0's JWKS keys.
    """
    return jwt.decode(id_token, options={"verify_signature": False, "verify_aud": False})


def ensure_logged_in():
    """
    Handle Auth0 login flow and store user info in st.session_state.
    If not logged in, show Login button and stop the app.
    """
    if "user" in st.session_state and "user_id" in st.session_state:
        return

    # Try to get 'code' from query params
    try:
        qp = st.query_params
    except AttributeError:
        qp = st.experimental_get_query_params()

    code = qp.get("code")
    if isinstance(code, list):
        code = code[0]

    if code:
        # Returned from Auth0 with a code
        try:
            token_data = exchange_code_for_token(code)
            id_token = token_data.get("id_token")
            if not id_token:
                raise ValueError("No id_token in Auth0 response.")
            claims = decode_id_token(id_token)

            auth0_sub = claims.get("sub")
            email = claims.get("email", "")

            if not auth0_sub:
                raise ValueError("Missing 'sub' in id_token.")

            user_row = get_or_create_user(auth0_sub, email)
            st.session_state["user"] = {
                "auth0_sub": auth0_sub,
                "email": email,
            }
            st.session_state["user_id"] = user_row["id"]

            # Clear 'code' from URL and rerun once
            try:
                st.experimental_set_query_params()
            except Exception:
                pass
            st.rerun()
        except Exception as e:
            st.error(f"❌ Login failed: {e}")
            st.stop()

    # Not logged in and no 'code' param -> show login link
    login_url = get_auth0_authorize_url()
    st.title("NBA Prop Analyzer")
    st.info("Please log in to view props, trends, and saved bets.")
    st.markdown(f"[🔐 Log in with Auth0]({login_url})")
    st.stop()


# ------------------------------------------------------
# REQUIRE LOGIN
# ------------------------------------------------------
ensure_logged_in()
user = st.session_state["user"]
user_id = st.session_state["user_id"]
st.sidebar.markdown(f"**User:** {user.get('email') or 'Logged in'}")

# ------------------------------------------------------
# LOGOS (STATIC)
# ------------------------------------------------------
TEAM_LOGOS = {
    "ATL": "https://a.espncdn.com/i/teamlogos/nba/500/atl.png",
    "BOS": "https://a.espncdn.com/i/teamlogos/nba/500/bos.png",
    "BKN": "https://a.espncdn.com/i/teamlogos/nba/500/bkn.png",
    "CHA": "https://a.espncdn.com/i/teamlogos/nba/500/cha.png",
    "CHI": "https://a.espncdn.com/i/teamlogos/nba/500/chi.png",
    "CLE": "https://a.espncdn.com/i/teamlogos/nba/500/cle.png",
    "DAL": "https://a.espncdn.com/i/teamlogos/nba/500/dal.png",
    "DEN": "https://a.espncdn.com/i/teamlogos/nba/500/den.png",
    "DET": "https://a.espncdn.com/i/teamlogos/nba/500/det.png",
    "GSW": "https://a.espncdn.com/i/teamlogos/nba/500/gs.png",
    "HOU": "https://a.espncdn.com/i/teamlogos/nba/500/hou.png",
    "IND": "https://a.espncdn.com/i/teamlogos/nba/500/ind.png",
    "LAC": "https://a.espncdn.com/i/teamlogos/nba/500/lac.png",
    "LAL": "https://a.espncdn.com/i/teamlogos/nba/500/lal.png",
    "MEM": "https://a.espncdn.com/i/teamlogos/nba/500/mem.png",
    "MIA": "https://a.espncdn.com/i/teamlogos/nba/500/mia.png",
    "MIL": "https://a.espncdn.com/i/teamlogos/nba/500/mil.png",
    "MIN": "https://a.espncdn.com/i/teamlogos/nba/500/min.png",
    "NOP": "https://a.espncdn.com/i/teamlogos/nba/500/no.png",
    "NYK": "https://a.espncdn.com/i/teamlogos/nba/500/ny.png",
    "OKC": "https://a.espncdn.com/i/teamlogos/nba/500/okc.png",
    "ORL": "https://a.espncdn.com/i/teamlogos/nba/500/orl.png",
    "PHI": "https://a.espncdn.com/i/teamlogos/nba/500/phi.png",
    "PHX": "https://a.espncdn.com/i/teamlogos/nba/500/phx.png",
    "POR": "https://a.espncdn.com/i/teamlogos/nba/500/por.png",
    "SAC": "https://a.espncdn.com/i/teamlogos/nba/500/sac.png",
    "SAS": "https://a.espncdn.com/i/teamlogos/nba/500/sa.png",
    "TOR": "https://a.espncdn.com/i/teamlogos/nba/500/tor.png",
    "UTA": "https://a.espncdn.com/i/teamlogos/nba/500/utah.png",
    "WAS": "https://a.espncdn.com/i/teamlogos/nba/500/wsh.png",
}

# ------------------------------------------------------
# SESSION STATE
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

if "saved_bets_loaded" not in st.session_state:
    st.session_state.saved_bets_loaded = False

if "active_tab" not in st.session_state:
    st.session_state.active_tab = "🧮 Props Overview"

if "trend_player" not in st.session_state:
    st.session_state.trend_player = None

if "trend_market" not in st.session_state:
    st.session_state.trend_market = None

if "trend_line" not in st.session_state:
    st.session_state.trend_line = None

if "trend_bet_type" not in st.session_state:
    st.session_state.trend_bet_type = None

# Load saved bets once per session, after we know user_id
if not st.session_state.saved_bets_loaded:
    st.session_state.saved_bets = load_saved_bets_from_db(user_id)
    st.session_state.saved_bets_loaded = True

# ------------------------------------------------------
# UTILITY FUNCTIONS
# ------------------------------------------------------
def format_moneyline(v):
    try:
        v = float(v)
        v_int = int(round(v))
        return f"+{v_int}" if v_int > 0 else str(v_int)
    except Exception:
        return "—"


def detect_stat(market):
    m = (market or "").lower()
    if "p+r+a" in m or "pra" in m:
        return "pra"
    if "assist" in m or "ast" in m:
        return "ast"
    if "reb" in m:
        return "reb"
    if "pt" in m or "point" in m:
        return "pts"
    return ""


def get_dynamic_averages(df):
    df = df.copy()

    def pull(row, n):
        stat = detect_stat(row["market"])
        if not stat:
            return np.nan
        col = f"{stat}_last{n}"
        return row.get(col, np.nan)

    df["L5 Avg"] = df.apply(lambda r: pull(r, 5), axis=1)
    df["L10 Avg"] = df.apply(lambda r: pull(r, 10), axis=1)
    df["L20 Avg"] = df.apply(lambda r: pull(r, 20), axis=1)
    return df


def add_defense(df):
    df = df.copy()
    stat_series = df["market"].apply(detect_stat)

    pos_cols = {
        "pts": "opp_pos_pts_rank",
        "reb": "opp_pos_reb_rank",
        "ast": "opp_pos_ast_rank",
        "pra": "opp_pos_pra_rank",
    }

    overall_cols = {
        "pts": "overall_pts_rank",
        "reb": "overall_reb_rank",
        "ast": "overall_ast_rank",
        "pra": "overall_pra_rank",
    }

    df["Pos Def Rank"] = [
        df.loc[i, pos_cols.get(stat_series[i])]
        if pos_cols.get(stat_series[i]) in df.columns
        else ""
        for i in df.index
    ]

    df["Overall Def Rank"] = [
        df.loc[i, overall_cols.get(stat_series[i])]
        if overall_cols.get(stat_series[i]) in df.columns
        else ""
        for i in df.index
    ]

    df["Matchup Difficulty"] = df.get("matchup_difficulty_score", np.nan)
    return df


def format_display(df):
    df = df.copy()

    # Round matchup difficulty for display
    df["Matchup Difficulty"] = df["Matchup Difficulty"].apply(
        lambda x: int(round(x)) if pd.notna(x) else ""
    )

    # Hit rate columns as percentages
    for col in ["hit_rate_last5", "hit_rate_last10", "hit_rate_last20"]:

        def fmt(x):
            if pd.isna(x):
                return ""
            if 0 <= x <= 1:
                return f"{int(round(x * 100))}%"
            return f"{int(round(x))}%"

        df[col] = df[col].apply(fmt)

    # Average columns as 1 decimal
    for col in ["L5 Avg", "L10 Avg", "L20 Avg"]:
        df[col] = df[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")

    return df


# ------------------------------------------------------
# LOAD DATA (BIGQUERY)
# ------------------------------------------------------
@st.cache_data
def load_props():
    df = bq_client.query(PROPS_SQL).to_dataframe()
    df.columns = df.columns.str.strip()

    df["game_date"] = (
        pd.to_datetime(df["game_date"], errors="coerce")
        .dt.tz_localize("UTC")
        .dt.tz_convert(EST)
    )

    df["home_team"] = df["home_team"].fillna("").astype(str)
    df["visitor_team"] = df["visitor_team"].fillna("").astype(str)
    df["opponent_team"] = df["opponent_team"].fillna("").astype(str)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["hit_rate_last10"] = pd.to_numeric(df["hit_rate_last10"], errors="coerce")

    for c in ["ev_last5", "ev_last10", "ev_last20"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


@st.cache_data
def load_historical():
    df = bq_client.query(HISTORICAL_SQL).to_dataframe()
    df.columns = df.columns.str.strip()

    df["game_date"] = (
        pd.to_datetime(df["game_date"], errors="coerce")
        .dt.tz_localize("UTC")
        .dt.tz_convert(EST)
    )
    df["opponent_team"] = df["opponent_team"].fillna("").astype(str)
    return df


props_df = load_props()
historical_df = load_historical()

# ------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------
st.sidebar.header("Filters")

games_list = (props_df["home_team"] + " vs " + props_df["visitor_team"]).astype(str)
games = ["All games"] + sorted(games_list.unique())
sel_game = st.sidebar.selectbox("Game", games)

players_sidebar = ["All players"] + sorted(
    props_df["player"].fillna("").astype(str).unique()
)
sel_player = st.sidebar.selectbox("Player", players_sidebar)

markets_sidebar = ["All Stats"] + sorted(
    props_df["market"].fillna("").astype(str).unique()
)
sel_market = st.sidebar.selectbox("Market", markets_sidebar)

books = sorted(props_df["bookmaker"].fillna("").astype(str).unique())
default_books = [b for b in books if b.lower() in ("draftkings", "fanduel")] or books
sel_books = st.sidebar.multiselect("Bookmaker", books, default=default_books)

od_min = int(props_df["price"].min())
od_max = int(props_df["price"].max())
sel_odds = st.sidebar.slider("Odds Range", od_min, od_max, (od_min, od_max))

sel_hit10 = st.sidebar.slider("Min Hit Rate L10", 0.0, 1.0, 0.5)

show_only_saved = st.sidebar.checkbox("Show Only Saved Props", value=False)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ------------------------------------------------------
# FILTER FUNCTION
# ------------------------------------------------------
def filter_props(df):
    d = df.copy()

    d["price"] = pd.to_numeric(d["price"], errors="coerce")
    d["hit_rate_last10"] = pd.to_numeric(d["hit_rate_last10"], errors="coerce")

    if sel_game != "All games":
        home, away = sel_game.split(" vs ")
        d = d[(d["home_team"] == home) & (d["visitor_team"] == away)]

    if sel_player != "All players":
        d = d[d["player"] == sel_player]

    if sel_market != "All Stats":
        d = d[d["market"] == sel_market]

    d = d[d["bookmaker"].isin(sel_books)]
    d = d[d["price"].between(sel_odds[0], sel_odds[1])]
    d = d[d["hit_rate_last10"] >= sel_hit10]

    if show_only_saved and st.session_state.saved_bets:
        saved_df = pd.DataFrame(st.session_state.saved_bets)
        key_cols = ["player", "market", "line", "bet_type", "bookmaker"]
        d = d.merge(saved_df[key_cols], on=key_cols, how="inner")

    return d


# ------------------------------------------------------
# TAB NAV (RADIO)
# ------------------------------------------------------
tab_labels = [
    "🧮 Props Overview",
    "📈 Trend Analysis",
    "📋 Saved Bets",
    "📊 Prop Analytics",
]

current_tab = st.radio(
    "View",
    tab_labels,
    index=tab_labels.index(st.session_state.active_tab),
    horizontal=True,
    key="active_tab",
)

# ------------------------------------------------------
# TAB 1 — PROPS OVERVIEW
# ------------------------------------------------------
if current_tab == "🧮 Props Overview":
    st.subheader("Props Overview")

    d = filter_props(props_df)
    if d.empty:
        st.info("No props match your filters.")
        st.stop()

    # ----------- Add dynamic averages + defense -----------
    d = get_dynamic_averages(d)
    d = add_defense(d)

    # ----------- FIX: Ensure hit rates are true numeric 0–100 -----------
    for col in ["hit_rate_last5", "hit_rate_last10", "hit_rate_last20"]:
        d[col] = (
            pd.to_numeric(d[col], errors="coerce")
            .apply(lambda x: x * 100 if 0 <= x <= 1 else x)
        )

    d["hit_rate_last10_num"] = d["hit_rate_last10"].astype(float)

    # ----------- Format display columns -----------
    d["Price"] = d["price"].apply(format_moneyline)
    d = format_display(d)
    d["Opponent Logo"] = d["opponent_team"].map(TEAM_LOGOS).fillna("")

    d_display = d.copy()

    # ----------- Mark saved bets -----------
    if st.session_state.saved_bets:
        saved_df = pd.DataFrame(st.session_state.saved_bets)
        key_cols = ["player", "market", "line", "bet_type", "bookmaker"]
        d_display["Save"] = d_display[key_cols].merge(
            saved_df[key_cols].drop_duplicates(),
            on=key_cols,
            how="left",
            indicator=True
        )["_merge"].eq("both")
    else:
        d_display["Save"] = False

    # ----------- Columns displayed -----------
    display_cols = [
        "Save",
        "player",
        "market",
        "line",
        "bet_type",
        "Price",
        "bookmaker",
        "Pos Def Rank",
        "Overall Def Rank",
        "Matchup Difficulty",
        "hit_rate_last5",
        "hit_rate_last10",
        "hit_rate_last20",
        "L5 Avg",
        "L10 Avg",
        "L20 Avg",
        "opponent_team",
    ]

    d_display = d_display[display_cols].rename(
        columns={
            "player": "Player",
            "market": "Market",
            "line": "Line",
            "bet_type": "Label",
            "bookmaker": "Book",
            "hit_rate_last5": "Hit L5",
            "hit_rate_last10": "Hit L10",
            "hit_rate_last20": "Hit L20",
            "opponent_team": "Opponent",
        }
    )

    # ----------- Streamlit Editor (with numeric columns) -----------
    edited = st.data_editor(
        d_display,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "Save": st.column_config.CheckboxColumn(
                "Save Bet", help="Save/unsave this prop"
            ),
            "Hit L5": st.column_config.NumberColumn("Hit L5", format="%.0f%%"),
            "Hit L10": st.column_config.NumberColumn("Hit L10", format="%.0f%%"),
            "Hit L20": st.column_config.NumberColumn("Hit L20", format="%.0f%%"),
            "L5 Avg": st.column_config.NumberColumn("L5 Avg", format="%.1f"),
            "L10 Avg": st.column_config.NumberColumn("L10 Avg", format="%.1f"),
            "L20 Avg": st.column_config.NumberColumn("L20 Avg", format="%.1f"),
            "Matchup Difficulty": st.column_config.NumberColumn(
                "Matchup Difficulty", format="%.0f"
            ),
        },
        key="props_overview_editor",
    )

    # ----------- SAFE checkbox handling (fixes NaN mask issue) -----------
    save_mask = edited["Save"].fillna(False).astype(bool)
    saved_rows = edited.loc[save_mask].copy()

    # ----------- Update session-state saved bets -----------
    if not saved_rows.empty:
        st.session_state.saved_bets = (
            saved_rows[["Player", "Market", "Line", "Label", "Price", "Book"]]
            .rename(
                columns={
                    "Player": "player",
                    "Market": "market",
                    "Line": "line",
                    "Label": "bet_type",
                    "Price": "price",
                    "Book": "bookmaker",
                }
            )
            .drop_duplicates()
            .to_dict("records")
        )
    else:
        st.session_state.saved_bets = []

    # ----------- Sync to PostgreSQL DB -----------
    replace_saved_bets_in_db(user_id, st.session_state.saved_bets)


# ------------------------------------------------------
# TAB 2 — TREND ANALYSIS
# ------------------------------------------------------
elif current_tab == "📈 Trend Analysis":
    st.subheader("Trend Analysis")

    # Player selector
    all_players = ["(select)"] + sorted(props_df["player"].unique())
    if st.session_state.trend_player in all_players:
        default_p_index = all_players.index(st.session_state.trend_player)
    else:
        default_p_index = 0

    p = st.selectbox("Player", all_players, index=default_p_index)
    if p == "(select)":
        st.stop()

    st.session_state.trend_player = p

    # Market selector
    markets = sorted(props_df[props_df["player"] == p]["market"].unique())
    if st.session_state.trend_market in markets:
        default_m_index = markets.index(st.session_state.trend_market)
    else:
        default_m_index = 0

    m = st.selectbox("Market", markets, index=default_m_index)
    st.session_state.trend_market = m

    # Bet type selector
    bet_types = sorted(
        props_df[(props_df["player"] == p) & (props_df["market"] == m)][
            "bet_type"
        ].dropna().unique()
    )
    if not bet_types:
        st.warning("No bet types available.")
        st.stop()

    if st.session_state.trend_bet_type in bet_types:
        default_bt = bet_types.index(st.session_state.trend_bet_type)
    else:
        default_bt = 0

    bt = st.selectbox("Bet Type", bet_types, index=default_bt)
    st.session_state.trend_bet_type = bt

    # Line selector
    lines = sorted(
        props_df[
            (props_df["player"] == p)
            & (props_df["market"] == m)
            & (props_df["bet_type"] == bt)
        ]["line"].unique()
    )

    if not lines:
        st.warning("No lines available.")
        st.stop()

    if st.session_state.trend_line in lines:
        default_line = list(lines).index(st.session_state.trend_line)
    else:
        default_line = 0

    line_pick = st.selectbox("Select Line", lines, index=default_line)
    st.session_state.trend_line = line_pick

    # ---------------------------
    # Sample size toggle
    # ---------------------------
    n_games = st.radio(
        "Sample Size (most recent games)",
        [5, 10, 20],
        index=1,
        horizontal=True,
    )

    # STAT DETECTION
    stat = detect_stat(m)
    if not stat:
        st.warning("Unable to detect stat type for this market.")
        st.stop()

    # Pull last N games
    df_hist = (
        historical_df[
            (historical_df["player"] == p) & (historical_df[stat].notna())
        ]
        .sort_values("game_date")
        .tail(n_games)
    )

    if df_hist.empty:
        st.info("No historical data available for this player/stat.")
        st.stop()

    df_hist["date"] = df_hist["game_date"].dt.strftime("%b %d")

    # OVER → stat > line ; UNDER → stat < line
    if bt.lower() == "over":
        hit_mask = df_hist[stat] > line_pick
    else:
        hit_mask = df_hist[stat] < line_pick

    df_hist["color"] = np.where(hit_mask, "green", "red")

    # ---------------------------
    # HIT RATE BADGE
    # ---------------------------
    total = len(df_hist)
    hits = int(hit_mask.sum())
    hit_rate = hits / total if total else 0

    st.markdown(
        f"### Hit Rate: **{hit_rate:.0%}**  \n"
        f"({hits} of {total} games) vs **{bt} {line_pick}**"
    )

    # ---------------------------
    # Plot
    # ---------------------------
    hover = []
    for dte, opp, val, hit in zip(
        df_hist["date"],
        df_hist["opponent_team"],
        df_hist[stat],
        hit_mask,
    ):
        hover.append(
            f"<b>{dte}</b><br>"
            f"{stat.upper()}: {val}<br>"
            f"Opponent: {opp}<br>"
            f"{'Hit' if hit else 'Miss'} vs {bt} {line_pick}"
        )

    fig = go.Figure()
    fig.add_bar(
        x=df_hist["date"],
        y=df_hist[stat],
        marker_color=df_hist["color"],
        hovertext=hover,
        hoverinfo="text",
    )

    fig.update_xaxes(tickvals=df_hist["date"], ticktext=df_hist["date"])

    fig.add_hline(
        y=line_pick,
        line_dash="dash",
        line_color="white",
        annotation_text=f"{bt} {line_pick}",
        annotation_position="top left",
    )

    fig.update_layout(
        height=450,
        plot_bgcolor="#222",
        paper_bgcolor="#222",
        font=dict(color="white"),
        margin=dict(b=40, t=40, l=40, r=20),
    )

    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------
# TAB 3 — SAVED BETS
# ------------------------------------------------------
elif current_tab == "📋 Saved Bets":
    st.subheader("Saved Bets")

    if not st.session_state.saved_bets:
        st.info("No saved bets yet. Go to Props Overview and check the 'Save Bet' boxes.")
    else:
        df_save = pd.DataFrame(st.session_state.saved_bets)

        df_save_display = df_save.rename(
            columns={
                "player": "Player",
                "market": "Market",
                "line": "Line",
                "bet_type": "Label",
                "price": "Price",
                "bookmaker": "Book",
            }
        )

        st.dataframe(df_save_display, use_container_width=True, hide_index=True)

        csv = df_save.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv, "saved_bets.csv", "text/csv")

# ------------------------------------------------------
# TAB 4 — PROP ANALYTICS
# ------------------------------------------------------
elif current_tab == "📊 Prop Analytics":
    st.subheader("Prop Analytics")

    d = filter_props(props_df)

    if d.empty:
        st.info("No props match your filters.")
    else:
        d = get_dynamic_averages(d)
        d = add_defense(d)
        d["Price"] = d["price"].apply(format_moneyline)

        ev_cols = ["ev_last5", "ev_last10", "ev_last20"]
        missing_ev = [c for c in ev_cols if c not in d.columns]
        if missing_ev:
            st.error(f"❌ Missing EV columns in database: {', '.join(missing_ev)}")
        else:
            for col in ev_cols:
                d[col] = pd.to_numeric(d[col], errors="coerce")

            d["Hit Rate 10"] = d["hit_rate_last10"]
            d = d.sort_values("ev_last10", ascending=False)

            cols = [
                "player",
                "market",
                "line",
                "Price",
                "bookmaker",
                "ev_last5",
                "ev_last10",
                "ev_last20",
                "Matchup Difficulty",
                "Hit Rate 10",
                "L10 Avg",
            ]

            d_display = d[cols].rename(
                columns={
                    "player": "Player",
                    "market": "Market",
                    "line": "Line",
                    "bookmaker": "Book",
                    "ev_last5": "EV L5",
                    "ev_last10": "EV L10",
                    "ev_last20": "EV L20",
                }
            )

            st.dataframe(d_display, use_container_width=True, hide_index=True)

# ------------------------------------------------------
# LAST UPDATED
# ------------------------------------------------------
now = datetime.now(EST)
st.sidebar.markdown(f"**Last Updated:** {now.strftime('%b %d, %I:%M %p')} ET")
