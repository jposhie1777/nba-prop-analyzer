# ------------------------------------------------------
# NBA Prop Analyzer - Merged Production + Dev UI
# ------------------------------------------------------
import os
import json
from datetime import datetime
from urllib.parse import urlencode

import base64
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytz
import requests
import streamlit as st
import psycopg2
import psycopg2.extras
import jwt
import streamlit.components.v1 as components
import textwrap


from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from google.cloud import bigquery
from google.oauth2 import service_account

# ------------------------------------------------------
# TIMEZONE (EST)
# ------------------------------------------------------
EST = pytz.timezone("America/New_York")

# ------------------------------------------------------
# STREAMLIT CONFIG
# ------------------------------------------------------
st.set_page_config(
    page_title="NBA Prop Analyzer",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")

DATASET = os.getenv("BIGQUERY_DATASET", "nba_prop_analyzer")
PROPS_TABLE = "todays_props_with_hit_rates"
HISTORICAL_TABLE = "historical_player_stats_for_trends"

# SERVICE_JSON is a JSON string (not a filepath)
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
FROM {PROJECT_ID}.{DATASET}.{PROPS_TABLE}
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
  stl,
  blk,
  pra,

  -- Last 5 (already arrays in your table)
  pts_last5_list,
  reb_last5_list,
  ast_last5_list,
  stl_last5_list,
  blk_last5_list,
  pra_last5_list,
  pr_last5_list,
  pa_last5_list,
  ra_last5_list,

  -- Last 7
  pts_last7_list,
  reb_last7_list,
  ast_last7_list,
  stl_last7_list,
  blk_last7_list,
  pra_last7_list,
  pr_last7_list,
  pa_last7_list,
  ra_last7_list,

  -- Last 10
  pts_last10_list,
  reb_last10_list,
  ast_last10_list,
  stl_last10_list,
  blk_last10_list,
  pra_last10_list,
  pr_last10_list,
  pa_last10_list,
  ra_last10_list

FROM `{PROJECT_ID}.{DATASET}.{HISTORICAL_TABLE}`
ORDER BY game_date DESC;
"""



# NEW: depth chart + injury SQL
DEPTH_SQL = f"""
SELECT
  team_number,
  team_abbr,
  team_name,
  player,
  position,
  role,
  depth
FROM {PROJECT_ID}.nba_data.team_rosters_2025_2026
ORDER BY team_number, position, depth
"""

INJURY_SQL = f"""
SELECT
  snapshot_ts,
  injury_id,
  report_date,
  player_id,
  first_name,
  last_name,
  full_name,
  team_id,
  team_abbrev,
  team_name,
  status,
  status_type_desc,
  status_type_abbr,
  return_date_raw,
  injury_type,
  injury_location,
  injury_side,
  injury_detail,
  long_comment,
  short_comment
FROM `{PROJECT_ID}.nba_prop_analyzer.player_injuries_raw`
ORDER BY snapshot_ts DESC
"""


# NEW: WOWY delta SQL
DELTA_SQL = f"""
SELECT *
FROM {PROJECT_ID}.nba_prop_analyzer.player_wowy_deltas
"""

# ------------------------------------------------------
# GAME ANALYTICS + GAME REPORT + GAME ODDS SQL
# ------------------------------------------------------

GAME_ANALYTICS_SQL = f"""
SELECT *
FROM `{PROJECT_ID}.nba_prop_analyzer.game_analytics`
ORDER BY game_date DESC, game_id
"""

GAME_REPORT_SQL = f"""
SELECT *
FROM `{PROJECT_ID}.nba_prop_analyzer.game_report`
ORDER BY game_id
"""

GAME_ODDS_SQL = f"""
SELECT
  Game,
  `Start Time` AS start_time,
  `Home Team` AS home_team,
  `Away Team` AS away_team,
  Bookmaker,
  Market,
  Outcome,
  Line,
  Price
FROM `{PROJECT_ID}.nba.nba_game_odds`
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
    """Create a new PostgreSQL connection to your Render database."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db_schema():
    """Create tables if they don't exist. Safe to run on every startup."""
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
    """Ensure a user exists in the 'users' table and return the row."""
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
    Normalize any old 'Label' keys to 'bet_type'.
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
                # Normalize: if old 'Label' is present, map it to 'bet_type'
                if "bet_type" not in details and "Label" in details:
                    details["bet_type"] = details.pop("Label")

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
            # Normalize 'Label' -> 'bet_type' just in case
            if "bet_type" not in bet and "Label" in bet:
                bet["bet_type"] = bet.pop("Label")

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
# THEME PRESETS (from dev)
# ------------------------------------------------------
THEMES = {
    "Sportsbook Dark": {
        "bg": "#020617",
        "accent": "#0ea5e9",
        "accent_soft": "#0369a1",
    },
    "Neon Night": {
        "bg": "#050816",
        "accent": "#a855f7",
        "accent_soft": "#22c55e",
    },
    "Slate Blue": {
        "bg": "#020617",
        "accent": "#3b82f6",
        "accent_soft": "#6366f1",
    },
}

if "theme_choice" not in st.session_state:
    st.session_state.theme_choice = "Sportsbook Dark"

theme_choice = st.sidebar.selectbox(
    "Theme",
    list(THEMES.keys()),
    index=list(THEMES.keys()).index(st.session_state.theme_choice),
    key="theme_choice",
)
theme = THEMES[st.session_state.theme_choice]

# ------------------------------------------------------
# GLOBAL STYLES (Optimized - Full Visual Preservation)
# ------------------------------------------------------
st.markdown(
    f"""
    <style>

    /* ---------- GLOBAL THEME ---------- */

    html, body, [class*="css"] {{
        font-family: "Inter", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}

    body {{
        background: radial-gradient(circle at top, {theme["bg"]} 0, #000 55%) !important;
    }}

    .block-container {{
        padding-top: 1rem !important;
        padding-bottom: 2rem !important;
        max-width: 1400px !important;
    }}

    [data-testid="stSidebar"] {{
        background: radial-gradient(circle at top left, #1f2937 0, #020617 55%);
        border-right: 1px solid rgba(255,255,255,0.04);
    }}

    [data-testid="stSidebar"] * {{
        color: #e5e7eb !important;
    }}

    /* ---------- HEADER ---------- */

    .app-header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0.6rem 0 1.1rem;
        border-bottom: 1px solid rgba(148,163,184,0.25);
        margin-bottom: 0.9rem;
    }}

    .app-header-left {{
        display: flex;
        align-items: center;
        gap: 0.85rem;
    }}

    .app-logo {{
        width: 42px;
        height: 42px;
        border-radius: 12px;
        background: radial-gradient(circle at 0 0, #f97316, #ea580c 25%, #0f172a 90%);
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-weight: 900;
        font-size: 20px;
        letter-spacing: 0.02em;
        box-shadow: 0 14px 30px rgba(15,23,42,0.8);
        animation: float-logo 5s ease-in-out infinite;
    }}

    @keyframes float-logo {{
        0%, 100% {{ transform: translateY(0); }}
        50% {{ transform: translateY(-2px); }}
    }}

    .app-title {{
        font-size: 1.4rem;
        font-weight: 700;
        color: #e5e7eb;
        margin: 0;
    }}

    .app-subtitle {{
        margin: 0;
        font-size: 0.78rem;
        color: #9ca3af;
    }}

    .pill {{
        padding: 4px 12px;
        border-radius: 999px;
        border: 1px solid rgba(148,163,184,0.4);
        font-size: 0.7rem;
        letter-spacing: 0.12em;
        color: #e5e7eb;
        background: linear-gradient(135deg, rgba(15,118,110,0.45), rgba(15,23,42,0.98));
        display: inline-flex;
        align-items: center;
        gap: 6px;
        box-shadow: 0 12px 30px rgba(15,23,42,0.9);
    }}

    .pill-dot {{
        width: 7px;
        height: 7px;
        border-radius: 999px;
        background: #22c55e;
        box-shadow: 0 0 10px rgba(34,197,94,0.9);
        animation: pulse-dot 1.5s infinite;
    }}

    @keyframes pulse-dot {{
        0%, 100% {{ transform: scale(1); opacity: 1; }}
        50% {{ transform: scale(1.35); opacity: 0.75; }}
    }}

    /* ---------- METRIC CARDS ---------- */

    .metric-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        gap: 0.75rem;
        margin-bottom: 0.75rem;
    }}

    .metric-card {{
        background: radial-gradient(circle at top, rgba(15,23,42,0.94), rgba(15,23,42,0.98));
        border-radius: 16px;
        padding: 0.75rem 0.9rem;
        border: 1px solid rgba(148,163,184,0.35);
        box-shadow: 0 18px 45px rgba(15,23,42,0.95);
        transition: 0.14s ease-out;
    }}

    .metric-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 24px 55px rgba(15,23,42,1);
        border-color: {theme["accent"]};
    }}

    .metric-label {{
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        color: #9ca3af;
    }}

    .metric-value {{
        font-size: 1.1rem;
        font-weight: 600;
        color: #f9fafb;
    }}

    .metric-sub {{
        font-size: 0.72rem;
        color: #9ca3af;
    }}

    /* ---------- PROP CARD (NEON NIGHT) ---------- */

    .prop-card {{
        position: relative;
        border-radius: 20px;
        padding: 1rem 1.15rem;
        border: 1px solid rgba(129,140,248,0.75);
        background:
            radial-gradient(circle at 0 0, rgba(168,85,247,0.22), transparent 55%),
            radial-gradient(circle at 100% 0, rgba(34,197,94,0.20), transparent 55%),
            radial-gradient(circle at 0 130%, rgba(15,23,42,1), rgba(15,23,42,0.95));
        box-shadow:
            0 18px 45px rgba(15,23,42,0.95),
            0 0 26px rgba(129,140,248,0.45);
        margin-bottom: 1rem;
        overflow: hidden;
        transition: 0.16s ease-out;
    }}

    .prop-card::before {{
        content: "";
        position: absolute;
        inset: 0;
        border-radius: inherit;
        border-top: 1px solid rgba(248,250,252,0.12);
        border-left: 1px solid rgba(248,250,252,0.06);
        pointer-events: none;
    }}

    .prop-card:hover {{
        transform: translateY(-4px);
        filter: saturate(1.15);
        border-color: {theme["accent"]};
        box-shadow:
            0 26px 70px rgba(15,23,42,1),
            0 0 35px rgba(168,85,247,0.60);
        background:
            radial-gradient(circle at 0 0, rgba(168,85,247,0.32), transparent 55%),
            radial-gradient(circle at 100% 0, rgba(34,197,94,0.26), transparent 55%),
            radial-gradient(circle at 0 130%, rgba(15,23,42,1), rgba(15,23,42,0.98));
    }}

    .prop-headline {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.45rem;
    }}

    .prop-player {{
        font-size: 1rem;
        font-weight: 700;
        color: #f9fafb;
    }}

    .prop-market {{
        color: #9ca3af;
        font-size: 0.8rem;
        white-space: nowrap;
    }}

    .pill-book {{
        padding: 3px 10px;
        font-size: 0.7rem;
        border-radius: 999px;
        border: 1px solid rgba(148,163,184,0.55);
        color: #e5e7eb;
        background: linear-gradient(135deg, rgba(15,23,42,0.1), rgba(88,28,135,0.9));
        display: inline-flex;
        align-items: center;
        gap: 4px;
        box-shadow: 0 0 18px rgba(168,85,247,0.55);
    }}

    .prop-meta {{
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 0.45rem;
        margin-top: 0.45rem;
    }}

    .prop-meta-label {{
        font-size: 0.68rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #6b7280;
    }}

    .prop-meta-value {{
        font-size: 0.84rem;
        font-weight: 650;
        color: #e5e7eb;
    }}

    /* ---------- DATAFRAME / TABLES ---------- */

    [data-testid="stDataFrame"],
    [data-testid="stDataEditor"] {{
        border-radius: 16px !important;
        border: 1px solid rgba(148,163,184,0.45) !important;
        box-shadow: 0 20px 50px rgba(15,23,42,0.98) !important;
        overflow: hidden;
        background: radial-gradient(circle at top left, rgba(15,23,42,0.98), rgba(15,23,42,0.96));
    }}

    .stDataFrame table,
    .stDataEditor table {{
        width: 100%;
        border-collapse: collapse;
    }}

    .stDataFrame thead th,
    .stDataEditor thead th {{
        background: #020617 !important;
        color: #e5e7eb !important;
        font-weight: 700 !important;
        border-bottom: 1px solid rgba(148,163,184,0.45) !important;
    }}

    .stDataFrame tbody tr:nth-child(even) td,
    .stDataEditor tbody tr:nth-child(even) td {{
        background: rgba(17,24,39,0.9) !important;
    }}

    .stDataFrame tbody tr:nth-child(odd) td,
    .stDataEditor tbody tr:nth-child(odd) td {{
        background: rgba(15,23,42,0.95) !important;
    }}

    .stDataFrame tbody tr:hover td,
    .stDataEditor tbody tr:hover td {{
        background: rgba(15,23,42,1) !important;
    }}

    /* ---------- BUTTONS ---------- */

    .stButton > button {{
        border-radius: 999px !important;
        padding: 0.35rem 0.95rem !important;
        font-weight: 600 !important;
        border: 1px solid rgba(148,163,184,0.4) !important;
        background: radial-gradient(circle at 0 0, {theme["accent"]}, {theme["accent_soft"]} 50%, #020617 100%);
        color: #f9fafb !important;
        box-shadow: 0 12px 30px rgba(8,47,73,0.9);
        transition: 0.16s ease-out !important;
    }}

    .stButton > button:hover {{
        transform: translateY(-1px) scale(1.01);
        box-shadow: 0 16px 40px rgba(8,47,73,1);
    }}

    .sparkline {{
        stroke: {theme["accent"]};
        fill: none;
    }}

    /* ---------- COLLAPSIBLE FILTER PANEL / COMPACT FILTERS ---------- */

    .filter-panel {{
        background-color: rgba(255,255,255,0.05);
        padding: 12px 18px;
        border-radius: 10px;
        margin-bottom: 15px;
        border: 1px solid rgba(255,255,255,0.08);
    }}

    div[data-baseweb="tag"] {{
        padding: 1px 6px !important;
        border-radius: 4px !important;
        font-size: 12px !important;
    }}

    div[data-baseweb="select"] > div {{
        min-height: 32px !important;
    }}

    .css-1n76uvr, 
    .css-1wa3eu0-placeholder {{
        font-size: 13px !important;
    }}

    .css-1wa3eu0-control, 
    .css-1y4p8pa-control {{
        min-height: 32px !important;
        border-radius: 6px !important;
    }}

    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------------------------------
# AG-GRID MOBILE FIX (separate block)
# ------------------------------------------------------
st.markdown(
    """
    <style>

    .ag-theme-balham .ag-center-cols-container {
        min-width: 1100px !important;
    }

    .ag-theme-balham .ag-body-viewport,
    .ag-theme-balham .ag-center-cols-viewport,
    .ag-theme-balham .ag-root-wrapper,
    .ag-theme-balham .ag-root {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch !important;
    }

    .ag-theme-balham .ag-header-cell,
    .ag-theme-balham .ag-cell {
        min-width: 115px !important;
        white-space: nowrap !important;
    }

    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------------------------------
# SCROLL-TO-TOP FLOATING BUTTON (GLOBAL)
# ------------------------------------------------------
st.markdown("""
<style>
#scrollTopBtn {
    position: fixed;
    bottom: 25px;
    right: 25px;
    z-index: 9999;
    background-color: rgba(30, 30, 30, 0.85);
    color: white;
    padding: 10px 14px;
    border-radius: 8px;
    cursor: pointer;
    font-size: 16px;
    border: 1px solid rgba(255,255,255,0.2);
    box-shadow: 0px 4px 12px rgba(0,0,0,0.4);
    backdrop-filter: blur(8px);
    display: none;
}
#scrollTopBtn:hover {
    background-color: rgba(60, 60, 60, 0.9);
}
</style>

<script>
window.addEventListener('scroll', function() {
    const btn = document.getElementById('scrollTopBtn');
    if (btn) {
        if (window.scrollY > 400) {
            btn.style.display = "block";
        } else {
            btn.style.display = "none";
        }
    }
});
function scrollToTop() {
    window.scrollTo({top: 0, behavior: 'smooth'});
}
</script>

<div id="scrollTopBtn" onclick="scrollToTop()">▲ Top</div>
""", unsafe_allow_html=True)

# ------------------------------------------------------
# SPORT SELECTOR (TOP, ABOVE HEADER)
# ------------------------------------------------------
sport = st.selectbox(
    "Sport",
    ["NBA", "NCAA Men's", "NCAA Women's"],
    index=0,
)


# ------------------------------------------------------
# HEADER
# ------------------------------------------------------
st.markdown(
    """
    <div class="app-header">
        <div class="app-header-left">
            <div class="app-logo">NBA</div>
            <div>
                <h1 class="app-title">Prop Analyzer</h1>
                <p class="app-subtitle">
                    Explore props, trends, saved bets, and team context using live BigQuery data.
                </p>
            </div>
        </div>
        <div>
            <span class="pill">
                <span class="pill-dot"></span>
                LIVE AUTHENTICATED
            </span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


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

# Map full team names from BigQuery → 3-letter codes
TEAM_NAME_TO_CODE = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "LA Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}

import os
import base64
import pathlib

# Directory containing this Python file
FILE_DIR = pathlib.Path(__file__).resolve().parent

# Correct logo directory
LOGO_DIR = FILE_DIR / "static" / "logos"

def logo_to_base64_local(path: str) -> str:
    try:
        with open(path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
            return f"data:image/png;base64,{encoded}"
    except Exception as e:
        print(f"Error loading logo {path}: {e}")
        return ""

SPORTSBOOK_LOGOS = {
    "DraftKings": str(LOGO_DIR / "Draftkingssmall.png"),
    "FanDuel": str(LOGO_DIR / "Fanduelsmall.png"),
}

SPORTSBOOK_LOGOS_BASE64 = {
    name: logo_to_base64_local(path)
    for name, path in SPORTSBOOK_LOGOS.items()
}

import os



MARKET_DISPLAY_MAP = {
    "player_assists_alternate": "Assists",
    "player_points_alternate": "Points",
    "player_rebounds_alternate": "Rebounds",
    "player_points_assists_alternate": "Pts+Ast",
    "player_points_rebounds_alternate": "Pts+Reb",
    "player_points_rebounds_assists_alternate": "PRA",
    "player_rebounds_assists_alternate": "Reb+Ast",

    # NEW:
    "player_steals_alternate": "Steals",
    "player_blocks_alternate": "Blocks",
}

def build_tags_html(tags):
    """
    Render tags on one line with NO indentation or newlines,
    so Streamlit does NOT interpret them as code blocks.
    """
    html_parts = []

    for label, color in tags:
        html_parts.append(
            f'<span style="background:{color};'
            f'padding:3px 8px;border-radius:8px;'
            f'margin-right:4px;font-size:0.68rem;'
            f'font-weight:600;color:white;display:inline-block;">'
            f'{label}</span>'
        )

    return "".join(html_parts)


# ------------------------------------------------------
# LOGO LOADERS
# ------------------------------------------------------

@st.cache_data(show_spinner=False)
def logo_to_base64_local(path: str) -> str:
    if not path:
        return ""
    try:
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
        return f"data:image/png;base64,{b64}"
    except:
        return ""

@st.cache_data(show_spinner=False)
def logo_to_base64_url(url: str) -> str:
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        b64 = base64.b64encode(r.content).decode("utf-8")
        return f"data:image/png;base64,{b64}"
    except:
        return ""

TEAM_LOGOS_BASE64 = {
    code: logo_to_base64_url(url)
    for code, url in TEAM_LOGOS.items()
}

SPORTSBOOK_LOGOS_BASE64 = {
    name: logo_to_base64_local(path)
    for name, path in SPORTSBOOK_LOGOS.items()
}


def normalize_team_code(raw: str) -> str:
    if raw is None:
        return ""

    s = str(raw).strip()

    if s.upper() in TEAM_LOGOS:
        return s.upper()

    if s in TEAM_NAME_TO_CODE:
        return TEAM_NAME_TO_CODE[s]

    s_low = s.lower()

    for full_name, code in TEAM_NAME_TO_CODE.items():
        if s_low in full_name.lower():
            return code

    for full_name, code in TEAM_NAME_TO_CODE.items():
        if full_name.lower().startswith(s_low):
            return code

    city_aliases = {
        "la": "LAL",
        "los angeles": "LAL",
        "new york": "NYK",
        "phoenix": "PHX",
        "golden state": "GSW",
        "san antonio": "SAS",
        "new orleans": "NOP",
        "oklahoma city": "OKC",
        "utah": "UTA",
        "cleveland": "CLE",
        "miami": "MIA",
        "milwaukee": "MIL",
    }

    for alias, code in city_aliases.items():
        if s_low == alias or s_low.startswith(alias):
            return code

    return s


# ------------------------------------------------------
# SESSION STATE
# ------------------------------------------------------
if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

if "saved_bets_loaded" not in st.session_state:
    st.session_state.saved_bets_loaded = False

# Trend lab state (optional)
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



# Helper: persist bets for this user to Postgres
def save_bet_for_user(user_id: int, bet: dict):
    """Append a bet for this user (per account) and sync to the saved_bets table."""
    # Normalize old 'Label' field if present
    if "bet_type" not in bet and "Label" in bet:
        bet["bet_type"] = bet.pop("Label")

    # Append to in-memory session list
    current = st.session_state.get("saved_bets", [])
    current.append(bet)
    st.session_state.saved_bets = current

    # Sync entire list to DB
    replace_saved_bets_in_db(user_id, current)

# ------------------------------------------------------
# UTILITY FUNCTIONS (from production)
# ------------------------------------------------------
def format_moneyline(v):
    try:
        v = float(v)
        v_int = int(round(v))
        return f"+{v_int}" if v_int > 0 else str(v_int)
    except Exception:
        return "—"


def detect_stat(market: str) -> str:
    """
    Map a props 'market' string to a stat key:
    pts, reb, ast, stl, blk, pra, pr, pa, ra
    Works for both pretty labels and internal codes like
    'player_points_rebounds_assists_alternate'.
    """
    m = (market or "").lower()

    # ---- COMBO STATS FIRST ----
    # PRA
    if (
        "points_rebounds_assists" in m
        or "p+r+a" in m
        or " pra" in m
        or m.endswith("pra")
        or "pra " in m
    ):
        return "pra"

    # P+R
    if (
        "points_rebounds" in m
        or ("p+r" in m and "a" not in m)
        or " pr" in m
        or m.endswith("pr")
    ):
        return "pr"

    # P+A
    if (
        "points_assists" in m
        or "points_and_assists" in m
        or "p+a" in m
        or " pa" in m
        or m.endswith("pa")
    ):
        return "pa"

    # R+A
    if (
        "rebounds_assists" in m
        or "rebounds_and_assists" in m
        or "r+a" in m
        or " ra" in m
        or m.endswith("ra")
    ):
        return "ra"

    # ---- SINGLE STATS ----
    if "assist" in m or "ast" in m:
        return "ast"
    if "rebound" in m or "reb" in m:
        return "reb"
    if "point" in m or "pts" in m or "scoring" in m:
        return "pts"
    if "stl" in m or "steal" in m:
        return "stl"
    if "blk" in m or "block" in m:
        return "blk"

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
        "stl": "opp_pos_stl_rank",
        "blk": "opp_pos_blk_rank",
    }

    overall_cols = {
        "pts": "overall_pts_rank",
        "reb": "overall_reb_rank",
        "ast": "overall_ast_rank",
        "pra": "overall_pra_rank",
        "stl": "overall_stl_rank",
        "blk": "overall_blk_rank",
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

    df["Matchup Difficulty"] = df["Matchup Difficulty"].apply(
        lambda x: int(round(x)) if pd.notna(x) else ""
    )

    for col in ["L5 Avg", "L10 Avg", "L20 Avg"]:
        df[col] = df[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")

    return df

# ------------------------------------------------------
# LOAD DATA (BIGQUERY)
# ------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_props():
    df = bq_client.query(PROPS_SQL).to_dataframe()
    df.columns = df.columns.str.strip()

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    for col in ["home_team", "visitor_team", "opponent_team"]:
        df[col] = df[col].fillna("").astype(str)

    # Core numerics
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["hit_rate_last5"] = pd.to_numeric(df.get("hit_rate_last5"), errors="coerce")
    df["hit_rate_last10"] = pd.to_numeric(df.get("hit_rate_last10"), errors="coerce")
    df["hit_rate_last20"] = pd.to_numeric(df.get("hit_rate_last20"), errors="coerce")

    # Handle renamed matchup difficulty column
    if "matchup_difficulty_by_stat" in df.columns:
        df["matchup_difficulty_score"] = pd.to_numeric(
            df["matchup_difficulty_by_stat"], errors="coerce"
        )
    else:
        df["matchup_difficulty_score"] = pd.to_numeric(
            df.get("matchup_difficulty_score"), errors="coerce"
        )

    # EV & edge / projection / minutes-usage numerics
    num_cols = [
        "ev_last5", "ev_last10", "ev_last20",
        "implied_prob", "edge_raw", "edge_pct",
        "proj_last10", "proj_std_last10", "proj_volatility_index",
        "proj_diff_vs_line",
        "est_minutes", "usage_bump_pct",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


import numpy as np
import ast

def convert_list_columns(df):
    for col in df.columns:
        if col.endswith(("_last5_list", "_last7_list", "_last10_list")):

            def fix(x):
                # Already list
                if isinstance(x, list):
                    return x

                # BigQuery gives numpy arrays
                if isinstance(x, np.ndarray):
                    return x.tolist()

                # Pandas Series
                if hasattr(x, "tolist"):
                    try:
                        return x.tolist()
                    except:
                        pass

                # Stringified lists
                if isinstance(x, str):
                    try:
                        return ast.literal_eval(x)
                    except:
                        return []

                # None
                if x is None:
                    return []

                return []

            df[col] = df[col].apply(fix)

    return df




@st.cache_data(show_spinner=True)
def load_history():
    df = bq_client.query(HISTORICAL_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["opponent_team"] = df["opponent_team"].fillna("").astype(str)

    # 🔥 Convert stringified lists into real Python lists
    df = convert_list_columns(df)

    return df

@st.cache_data(show_spinner=True)
def load_depth_charts():
    df = bq_client.query(DEPTH_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    return df


@st.cache_data(show_spinner=True)
def load_injury_report():
    df = bq_client.query(INJURY_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"], errors="coerce")
    return df


@st.cache_data(show_spinner=True)
def load_wowy_deltas():
    df = bq_client.query(DELTA_SQL).to_dataframe()
    df.columns = df.columns.str.strip()

    # Normalize player name for matching
    def norm(x):
        if not isinstance(x, str):
            return ""
        return (
            x.lower()
             .replace(".", "")
             .replace("-", " ")
             .replace("'", "")
             .strip()
        )

    df["player_norm"] = df["player_a"].apply(norm)
    return df

@st.cache_data(show_spinner=True)
def load_game_analytics():
    df = bq_client.query(GAME_ANALYTICS_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df

@st.cache_data(show_spinner=True)
def load_game_report():
    df = bq_client.query(GAME_REPORT_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df

@st.cache_data(show_spinner=True)
def load_game_odds():
    df = bq_client.query(GAME_ODDS_SQL).to_dataframe()
    df.columns = df.columns.str.strip()

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["Line"] = pd.to_numeric(df["Line"], errors="coerce")

    return df


# ------------------------------------------------------
# LOAD BASE TABLES
# ------------------------------------------------------
props_df = load_props()
history_df = load_history()
depth_df = load_depth_charts()
injury_df = load_injury_report()    # <-- MUST COME BEFORE FIX
wowy_df = load_wowy_deltas()
game_analytics_df = load_game_analytics()
game_report_df = load_game_report()
game_odds_df = load_game_odds()


# ------------------------------------------------------
# GLOBAL FILTER LISTS (used by Tab 1 & Tab 2)
# ------------------------------------------------------


market_list = sorted(props_df["market"].dropna().unique().tolist())
sportsbook_list = sorted(props_df["bookmaker"].dropna().unique().tolist())

# Build game list from home_team + visitor_team
games_today = sorted(
    (
        props_df["home_team"].astype(str)
        + " vs "
        + props_df["visitor_team"].astype(str)
    )
    .dropna()
    .unique()
    .tolist()
)

# Pretty market names for filters
market_pretty_map = {
    "player_points": "Points",
    "player_points_alternate": "Points (Alt)",
    "player_rebounds": "Rebounds",
    "player_rebounds_alternate": "Rebounds (Alt)",
    "player_assists": "Assists",
    "player_assists_alternate": "Assists (Alt)",
    "player_blocks": "Blocks",
    "player_blocks_alternate": "Blocks (Alt)",
    "player_steals": "Steals",
    "player_steals_alternate": "Steals (Alt)",
    "player_points_rebounds": "PTS + REB",
    "player_points_assists": "PTS + AST",
    "player_rebounds_assists": "REB + AST",
    "player_points_rebounds_assists": "PRA",
    "player_fg3m": "3PM",
    "player_blocks_steals": "Stocks (BLK+STL)"
}

# ------------------------------------------------------
# NORMALIZE PLAYER NAMES (fix merge issues)
# ------------------------------------------------------
def normalize_name(n):
    if not isinstance(n, str):
        return ""
    return (
        n.lower()
         .replace(".", "")
         .replace("'", "")
         .replace("-", " ")
         .replace(" jr", "")
         .replace(" sr", "")
         .strip()
    )

props_df["player_norm"] = props_df["player"].apply(normalize_name)
history_df["player_norm"] = history_df["player"].apply(normalize_name)


# ------------------------------------------------------
# ATTACH LAST-5 / LAST-7 / LAST-10 ARRAYS
# select MOST RECENT game per player (correct!)
# ------------------------------------------------------
hist_latest = (
    history_df.sort_values("game_date", ascending=False)
    .groupby("player_norm")
    .head(1)[[
        "player_norm",

        # base stats
        "pts_last5_list", "pts_last7_list", "pts_last10_list",
        "reb_last5_list", "reb_last7_list", "reb_last10_list",
        "ast_last5_list", "ast_last7_list", "ast_last10_list",
        "stl_last5_list", "stl_last7_list", "stl_last10_list",
        "blk_last5_list", "blk_last7_list", "blk_last10_list",

        # combo: PRA
        "pra_last5_list", "pra_last7_list", "pra_last10_list",

        # combo: PR, PA, RA  ✅ NEW
        "pr_last5_list",  "pr_last7_list",  "pr_last10_list",
        "pa_last5_list",  "pa_last7_list",  "pa_last10_list",
        "ra_last5_list",  "ra_last7_list",  "ra_last10_list",
    ]]
)

# Merge into props (so card_df rows have all lists)
props_df = props_df.merge(hist_latest, on="player_norm", how="left")



# ------------------------------------------------------
# FIX INJURY TEAM MATCHING (NEW SCHEMA)
# ------------------------------------------------------
def normalize(s):
    if not isinstance(s, str):
        return ""
    return (
        s.lower()
         .replace(".", "")
         .replace("'", "")
         .replace("-", " ")
         .strip()
    )

# Clean name columns
injury_df["first_clean"] = injury_df["first_name"].apply(normalize)
injury_df["last_clean"] = injury_df["last_name"].apply(normalize)

# Depth chart name normalization
depth_df["player_norm"] = depth_df["player"].apply(normalize)
depth_df["first_initial"] = depth_df["player_norm"].apply(
    lambda x: x.split(" ")[0][0] if x else ""
)
depth_df["last_clean"] = depth_df["player_norm"].apply(
    lambda x: x.split(" ")[-1] if len(x.split(" ")) >= 2 else ""
)

# Merge by team + last name (can fail for some rookies)
merged = injury_df.merge(
    depth_df,
    left_on=["team_abbrev", "last_clean"],
    right_on=["team_abbr", "last_clean"],
    how="left",
    suffixes=("", "_roster")
)

# Instead of DROPPING unmatched rows → keep all
def row_matches(row):
    fc = row.get("first_clean", "")
    inj_initial = fc[0] if isinstance(fc, str) and fc else ""
    roster_initial = row.get("first_initial", "")
    return inj_initial == roster_initial

merged["name_match"] = merged.apply(row_matches, axis=1)

# ✅ KEEP ALL ROWS (not just matched)
inj_fixed = merged.copy()

# Final column order
injury_df = inj_fixed[
    [
        "snapshot_ts",
        "injury_id",
        "report_date",
        "player_id",
        "first_name",
        "last_name",
        "full_name",
        "team_id",
        "team_abbrev",
        "team_name",
        "status",
        "status_type_desc",
        "status_type_abbr",
        "return_date_raw",
        "injury_type",
        "injury_location",
        "injury_side",
        "injury_detail",
        "long_comment",
        "short_comment",
        # depth fields (may be NaN for rookies)
        "team_number",
        "team_abbr",
    ]
]


# ------------------------------------------------------
# WOWY HELPERS
# ------------------------------------------------------
def attach_wowy_deltas(df, wowy_df_global):
    """Attach WOWY rows (can be multiple injured teammates) to props by player + team."""
    if wowy_df_global is None or wowy_df_global.empty:
        return df

    df = df.copy()

    def norm(x):
        if not isinstance(x, str):
            return ""
        return (
            x.lower()
             .replace(".", "")
             .replace("-", " ")
             .replace("'", "")
             .strip()
        )

    df["player_norm"] = df["player"].apply(norm)

    merged = df.merge(
        wowy_df_global,
        how="left",
        left_on=["player_norm", "player_team"],
        right_on=["player_norm", "team_abbr"],
        suffixes=("", "_wowy"),
    )
    return merged


def group_wowy_rows(df):
    """
    Reduce to one row per (player, market, line, bookmaker),
    but attach a DataFrame of all WOWY rows as _rows.
    """
    df = df.copy()
    group_cols = ["player", "market", "line", "bookmaker"]
    if not all(col in df.columns for col in group_cols):
        return df

    grouped_rows = []
    for _, g in df.groupby(group_cols):
        base = g.iloc[0].copy()
        base["_rows"] = g  # all WOWY variants in one blob
        grouped_rows.append(base)

    return pd.DataFrame(grouped_rows)


def market_to_delta_column(market):
    m = str(market).lower()
    if "point" in m or "pts" in m:
        return "pts_delta"
    if "reb" in m:
        return "reb_delta"
    if "ast" in m:
        return "ast_delta"
    if "pra" in m or "p+r+a" in m:
        return "pra_delta"
    if "p+r" in m:
        return "pts_reb_delta"
    if "p+a" in m or "pa" in m:
        return "pa_delta"
    if "r+a" in m or "ra" in m:
        return "ra_delta"
    
    return None


def build_wowy_block(row):
    """
    Show a SHORT summary:
    'Impact: +1.65 when Trae Young (Out)'
    Uses ONLY the largest WOWY delta for the current prop's stat.
    """
    delta_col = market_to_delta_column(row.get("market", ""))
    if not delta_col:
        return ""

    wrows = row.get("_wowy_list", [])
    if not wrows:
        return ""

    # Pick biggest absolute delta
    best = None
    best_val = 0

    for w in wrows:
        if delta_col in w and pd.notna(w[delta_col]):
            val = float(w[delta_col])
            if abs(val) > abs(best_val):
                best = w
                best_val = val

    if not best:
        return ""

    sign = "+" if best_val > 0 else ""
    color = "#22c55e" if best_val > 0 else "#ef4444"

    # Extract teammate + injury from breakdown ("Trae Young (Out) → ...")
    teammate = best["breakdown"].split("→")[0].strip()

    return f"""
        <div style="margin-top:6px; font-size:0.72rem;
                    padding:6px 10px;
                    border-radius:8px;
                    background:rgba(255,255,255,0.05);
                    border-left:3px solid {color};">
            <span style="color:{color}; font-weight:700;">
                Impact: {sign}{best_val:.2f}
            </span>
            <span style="color:#e5e7eb;">
                when {teammate}
            </span>
        </div>
    """

# ------------------------------------------------------
# SHARED CARD-GRID HELPERS (EV+ & Available Props)
# ------------------------------------------------------
import pandas as pd
import numpy as np

def get_spark_values(row):
    """
    Pick the best series for this prop, based on the detected stat.
    Priority: last10_list, then last7_list, then last5_list.
    Returns a plain Python list of numbers, or [].
    """
    stat = detect_stat(row.get("market", ""))  # pts, reb, ast, pra, stl, blk
    if not stat:
        return []

    candidates = [
        f"{stat}_last10_list",
        f"{stat}_last7_list",
        f"{stat}_last5_list",
    ]

    for col in candidates:
        if col not in row.index:
            continue

        vals = row[col]

        # Already a list
        if isinstance(vals, list):
            clean = [v for v in vals if isinstance(v, (int, float))]
            if clean:
                return clean

        # Numpy array
        if isinstance(vals, np.ndarray):
            clean = [float(v) for v in vals if isinstance(v, (int, float, np.number))]
            if clean:
                return clean

    return []


def build_sparkline_bars_hitmiss(values, line_value, width=90, height=34):
    """
    Mini bar chart with green/red coloring based on line hit,
    plus tiny numeric labels above each bar.
    """
    if not values or not isinstance(values, (list, tuple)):
        return ""

    values = [v for v in values if isinstance(v, (int, float))]
    if not values:
        return ""

    n = len(values)
    bar_width = width / n

    max_v = max(max(values), line_value)
    min_v = min(min(values), line_value)
    span = (max_v - min_v) or 1

    rects = []
    labels = []
    line_elems = []

    for i, v in enumerate(values):
        bar_height = (v - min_v) / span * (height - 12)
        x = i * bar_width
        y = height - bar_height

        # Color: green if hit, red if missed
        bar_color = "#22c55e" if v >= line_value else "#ef4444"

        rects.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width - 1:.1f}" '
            f'height="{bar_height:.1f}" fill="{bar_color}" rx="2" />'
        )

        labels.append(
            f'<text x="{x + bar_width/2:.1f}" y="{y - 2:.1f}" '
            f'font-size="6px" fill="#e5e7eb" text-anchor="middle">{int(v)}</text>'
        )

    # Line marker at prop line
    line_y = height - ((line_value - min_v) / span * (height - 12))
    line_elems.append(
        f'<line x1="0" y1="{line_y:.1f}" x2="{width}" y2="{line_y:.1f}" '
        f'stroke="#9ca3af" stroke-width="1" stroke-dasharray="3,2" />'
    )

    svg = f"""
    <svg width="{width}" height="{height}" style="overflow:visible;">
        {''.join(labels)}
        {''.join(rects)}
        {''.join(line_elems)}
    </svg>
    """
    return svg


def normalize_bookmaker(raw: str) -> str:
    if not raw:
        return ""
    r = raw.strip().lower()

    mapping = {
        "draft": "DraftKings",
        "draftkings": "DraftKings",
        "dk": "DraftKings",

        "fanduel": "FanDuel",
        "fd": "FanDuel",
        "fan duel": "FanDuel",
    }

    for k, v in mapping.items():
        if k in r:
            return v

    return raw.strip().title()


def get_l10_avg(row):
    """
    Keep existing behavior: use L10 numeric averages where available.
    """
    stat = detect_stat(row.get("market", ""))

    col_map = {
        "pts": "pts_last10",
        "reb": "reb_last10",
        "ast": "ast_last10",
        "pra": "pra_last10",
        "stl": "stl_last10",
        "blk": "blk_last10",
        "pr": "pr_last10",
        "pa": "pa_last10",
        "ra": "ra_last10",
    }

    col = col_map.get(stat)
    value = row.get(col)
    return float(value) if pd.notna(value) else None


def get_opponent_rank(row):
    stat = detect_stat(row.get("market", ""))
    col = {
        "pts": "opp_pos_pts_rank",
        "reb": "opp_pos_reb_rank",
        "ast": "opp_pos_ast_rank",
        "pra": "opp_pos_pra_rank",
        "stl": "opp_pos_stl_rank",
        "pr": "opp_pos_pr_rank",
        "pa": "opp_pos_pa_rank",
        "ra": "opp_pos_ra_rank",
        "blk": "opp_pos_blk_rank",
    }.get(stat)
    if col and col in row and pd.notna(row[col]):
        return int(row[col])
    return None


def rank_to_color(rank):
    if not isinstance(rank, int):
        return "#9ca3af"
    t = (rank - 1) / 29
    return f"hsl({120 * t}, 85%, 45%)"


def build_prop_tags(row):
    tags = []
    r = get_opponent_rank(row)
    if isinstance(r, int):
        if r <= 10:
            tags.append(("🔴 Hard", "#ef4444"))
        elif r <= 20:
            tags.append(("🟡 Neutral", "#eab308"))
        else:
            tags.append(("🟢 Easy", "#22c55e"))
    return tags


def compute_implied_prob(odds):
    try:
        odds = float(odds)
    except Exception:
        return None
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)

# Global injury lookup populated ONCE per app refresh
INJURY_LOOKUP_BY_NAME = {}

def build_injury_lookup():
    global INJURY_LOOKUP_BY_NAME

    if injury_df.empty:
        INJURY_LOOKUP_BY_NAME = {}
        return

    # Build lookup from ALL injuries — not just selected team
    tmp = injury_df.copy()

    def norm(s):
        return str(s).lower().replace("'", "").replace(".", "").replace("-", "").strip()

    tmp["full_norm"] = tmp.apply(
        lambda r: norm(f"{r['first_name']} {r['last_name']}"), axis=1
    )

    # Most recent injury entry per player
    tmp = (
        tmp.sort_values("snapshot_ts")
           .groupby("full_norm")
           .tail(1)
    )

    # Only store non-healthy statuses
    lookup = {}
    for _, r in tmp.iterrows():
        st = str(r.get("status", "")).strip()
        if st.lower() not in ["", "available", "healthy", "active"]:
            lookup[r["full_norm"]] = st

    INJURY_LOOKUP_BY_NAME = lookup

# Build lookup at load
build_injury_lookup()


def render_prop_cards(
    df,
    *,
    require_ev_plus: bool,
    odds_min: float,
    odds_max: float,
    min_hit_rate: float,
    hit_rate_col: str = "hit_rate_last10",
    hit_label: str = "L10 Hit",
    min_opp_rank: int | None = None,
    page_key: str = "ev",
):
    """
    Shared card-grid renderer for both EV+ Props and Available Props.
    df should already have basic cleaning done (price, hit_rate cols numeric).
    """

    if df.empty:
        st.info("No props match your filters.")
        return

    # ---- WOWY merge once per render ----
    card_df = attach_wowy_deltas(df, wowy_df)

    wowy_cols = [
        "breakdown", "pts_delta", "reb_delta", "ast_delta",
        "pra_delta", "pts_reb_delta"
    ]

    def extract_wowy_list(g):
        if not wowy_cols:
            return []
        df2 = g.copy()
        df2 = df2[wowy_cols]
        if "breakdown" in df2.columns:
            df2 = df2[df2["breakdown"].notna()]
        return df2.to_dict("records")

    # Build WOWY map
    w_map = {}
    for (player, team), g in card_df.groupby(["player", "player_team"]):
        w_map[(player, team)] = extract_wowy_list(g)

    card_df["_wowy_list"] = card_df.apply(
        lambda r: w_map.get((r["player"], r["player_team"]), []),
        axis=1
    )

    # ---- row filter ----
    def card_good(row):
        price = row.get("price")
        hit = row.get(hit_rate_col)

        if pd.isna(price) or pd.isna(hit):
            return False

        if not (odds_min <= price <= odds_max):
            return False

        if hit < min_hit_rate:
            return False

        if min_opp_rank is not None:
            r = get_opponent_rank(row)
            if r is None or r < min_opp_rank:
                return False

        if require_ev_plus:
            implied = compute_implied_prob(price)
            if implied is None or hit <= implied:
                return False

        return True

    card_df = card_df[card_df.apply(card_good, axis=1)]

    if card_df.empty:
        st.info("No props match your filters (after EV/odds/hit-rate logic).")
        return

    # ---- Sorting: best hit-rate → best odds ----
    card_df = card_df.sort_values(
        by=[hit_rate_col, "price"],
        ascending=[False, True]
    ).reset_index(drop=True)

    # ---- Pagination ----
    page_size = 30
    total_cards = len(card_df)
    total_pages = max(1, (total_cards + page_size - 1) // page_size)

    st.write(f"Showing {total_cards} props • {total_pages} pages")

    page = st.number_input(
        "Page",
        min_value=1,
        max_value=total_pages,
        value=1,
        step=1,
        key=f"{page_key}_card_page_number",
    )

    start = (page - 1) * page_size
    end = start + page_size
    page_df = card_df.iloc[start:end]

    st.markdown(
        '<div style="max-height:1100px; overflow-y:auto; padding-right:12px;">',
        unsafe_allow_html=True,
    )

    cols = st.columns(4)

    # ==============================
    # CARD LOOP
    # ==============================
    for idx, row in page_df.iterrows():
        col = cols[idx % 4]
        with col:

            # -------------------------------
            # Player
            # -------------------------------
            player = row.get("player", "")

            # Injury Badge (Step 2)
            def _norm(s):
                return str(s).lower().replace("'", "").replace(".", "").replace("-", "").strip()

            inj_status = INJURY_LOOKUP_BY_NAME.get(_norm(player))
            badge_html = ""

            if inj_status:
                s = inj_status.lower()
                if "out" in s:
                    badge_color = "#ef4444"        # red
                elif "question" in s or "doubt" in s:
                    badge_color = "#eab308"        # yellow
                else:
                    badge_color = "#3b82f6"        # blue

                badge_html = f"""
                    <span style="
                        background:{badge_color};
                        color:white;
                        padding:2px 6px;
                        font-size:0.65rem;
                        font-weight:700;
                        border-radius:6px;
                        margin-left:6px;
                        white-space:nowrap;
                    ">
                    {inj_status.upper()}
                    </span>
                """

            # -------------------------------
            # Basic fields
            # -------------------------------
            pretty_market = MARKET_DISPLAY_MAP.get(row.get("market", ""), row.get("market", ""))
            bet_type = str(row.get("bet_type", "")).upper()
            line = row.get("line", "")

            odds = int(row.get("price", 0))
            implied_prob = compute_implied_prob(odds) or 0.0
            hit = row.get(hit_rate_col, 0.0)

            l10_avg = get_l10_avg(row)
            l10_avg_display = f"{l10_avg:.1f}" if l10_avg is not None else "-"

            opp_rank = get_opponent_rank(row)
            if isinstance(opp_rank, int):
                rank_display = opp_rank
                rank_color = rank_to_color(opp_rank)
            else:
                rank_display = "-"
                rank_color = "#9ca3af"

            spark_vals = get_spark_values(row)
            line_value = float(row.get("line", 0))
            spark_html = build_sparkline_bars_hitmiss(spark_vals, line_value)

            # Logos
            player_team = normalize_team_code(row.get("player_team", ""))
            opp_team = normalize_team_code(row.get("opponent_team", ""))

            home_logo = TEAM_LOGOS_BASE64.get(player_team, "")
            opp_logo = TEAM_LOGOS_BASE64.get(opp_team, "")

            # Sportsbook Logo
            book = normalize_bookmaker(row.get("bookmaker", ""))
            book_logo_b64 = SPORTSBOOK_LOGOS_BASE64.get(book)

            if book_logo_b64:
                book_html = (
                    f'<img src="{book_logo_b64}" '
                    'style="height:26px; width:auto; max-width:80px; object-fit:contain;'
                    'filter:drop-shadow(0 0 6px rgba(0,0,0,0.4));" />'
                )
            else:
                book_html = (
                    '<div style="padding:3px 10px; border-radius:8px;'
                    'background:rgba(255,255,255,0.08);'
                    'border:1px solid rgba(255,255,255,0.15);'
                    'font-size:0.7rem;">'
                    f'{book}'
                    '</div>'
                )

            # Tags + WOWY
            tags_html = build_tags_html(build_prop_tags(row))
            wowy_html = build_wowy_block(row)

            # ---------- Card Layout ----------
            card_lines = [
                '<div class="prop-card">',

                # TOP BAR
                '  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">',

                # LEFT → MATCHUP LOGOS
                '    <div style="display:flex; align-items:center; gap:6px; min-width:70px;">'
                f'      <img src="{home_logo}" style="height:20px;border-radius:4px;" />'
                '      <span style="font-size:0.7rem;color:#9ca3af;">vs</span>'
                f'      <img src="{opp_logo}" style="height:20px;border-radius:4px;" />'
                '    </div>',

                # CENTER → PLAYER + MARKET + INJ BADGE
                '    <div style="text-align:center; flex:1; display:flex; flex-direction:column; align-items:center;">'
                f'      <div style="font-size:1.05rem;font-weight:700; display:flex; align-items:center;">{player}{badge_html}</div>'
                f'      <div style="font-size:0.82rem;color:#9ca3af;">{pretty_market} • {bet_type} {line}</div>'
                '    </div>',

                # RIGHT → BOOK
                '    <div style="display:flex; justify-content:flex-end; min-width:70px;">'
                f'      {book_html}'
                '    </div>',

                '  </div>',  # END TOP BAR

                # SPARKLINE
                '  <div style="display:flex; justify-content:center; margin:8px 0;">'
                    + spark_html +
                '  </div>',

                # TAGS
                '  <div style="display:flex; justify-content:center; margin-bottom:6px;">',
                f'    {tags_html}',
                '  </div>',

                # BOTTOM METRICS
                '  <div class="prop-meta" style="margin-top:2px;">',

                '    <div>',
                f'      <div style="color:#e5e7eb;font-size:0.8rem;">{odds:+d}</div>'
                f'      <div style="font-size:0.7rem;">Imp: {implied_prob:.0%}</div>',
                '    </div>',

                '    <div>',
                f'      <div style="color:#e5e7eb;font-size:0.8rem;">{hit_label}: {hit:.0%}</div>'
                f'      <div style="font-size:0.7rem;">L10 Avg: {l10_avg_display}</div>',
                '    </div>',

                '    <div>',
                f'      <div style="color:{rank_color};font-size:0.8rem;font-weight:700;">{rank_display}</div>',
                '      <div style="font-size:0.7rem;">Opp Rank</div>',
                '    </div>',

                '  </div>',  # END BOTTOM METRICS

                f'  {wowy_html}',

                '</div>',
            ]

            card_html = "\n".join(card_lines)
            st.markdown(card_html, unsafe_allow_html=True)

            # --- SAVE BET BUTTON ---
            bet_payload = {
                "player": player,
                "market": row.get("market"),
                "line": row.get("line"),
                "bet_type": bet_type,
                "price": odds,
                "bookmaker": row.get("bookmaker"),
            }

            btn_key = f"{page_key}_save_{player}_{row.get('market')}_{row.get('line')}_{idx}"

            if st.button("💾 Save Bet", key=btn_key):
                save_bet_for_user(user_id, bet_payload)
                st.success(f"Saved: {player} {pretty_market} {bet_type} {line}")

    st.markdown("</div>", unsafe_allow_html=True)



# ------------------------------------------------------
# SIDEBAR FILTERS (using production-style filters)
# ------------------------------------------------------
st.sidebar.header("Filters")

#games_list = (props_df["home_team"] + " vs " + props_df["visitor_team"]).astype(str)
#games = ["All games"] + sorted(games_list.unique())
#sel_game = st.sidebar.selectbox("Game", games)

#players_sidebar = ["All players"] + sorted(
    #props_df["player"].fillna("").astype(str).unique()
#)
#sel_player = st.sidebar.selectbox("Player", players_sidebar)

#markets_sidebar = ["All Stats"] + sorted(
    #props_df["market"].fillna("").astype(str).unique()
#)
#sel_market = st.sidebar.selectbox("Market", markets_sidebar)

#books = sorted(props_df["bookmaker"].fillna("").astype(str).unique())
#default_books = [b for b in books if b.lower() in ("draftkings", "fanduel")] or books
#sel_books = st.sidebar.multiselect("Bookmaker", books, default=default_books)

#od_min = int(props_df["price"].min()) if not props_df.empty else -300
#od_max = int(props_df["price"].max()) if not props_df.empty else 300
#sel_odds = st.sidebar.slider("Odds Range", od_min, od_max, (od_min, od_max))

#sel_hit10 = st.sidebar.slider("Min Hit Rate L10", 0.0, 1.0, 0.5)

#show_only_saved = st.sidebar.checkbox("Show Only Saved Props", value=False)

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ----------------------------------
# CARD GRID FILTERS (Manual Inputs)
# ----------------------------------
st.sidebar.markdown("### Card Grid Filters")

show_defensive_props = st.sidebar.checkbox(
    "Show Defensive Props (Steals & Blocks)",
    value=True
)


def filter_props(df):
    """
    Minimal shared filtering + preprocessing.
    All tab-specific UI filters are handled inside each tab.
    """
    d = df.copy()

    # safely handle global toggle (default False if not defined)
    show_only_saved_local = globals().get("show_only_saved", False)

    # ---------- Numeric cleanup ----------
    if "price" in d.columns:
        d["price"] = pd.to_numeric(d["price"], errors="coerce")

    if "hit_rate_last10" in d.columns:
        d["hit_rate_last10"] = pd.to_numeric(d["hit_rate_last10"], errors="coerce")

    if "hit_rate_last5" in d.columns:
        d["hit_rate_last5"] = pd.to_numeric(d["hit_rate_last5"], errors="coerce")

    if "hit_rate_last20" in d.columns:
        d["hit_rate_last20"] = pd.to_numeric(d["hit_rate_last20"], errors="coerce")

    # ------------------------------------------------------
    # Saved Bets Filter
    # ------------------------------------------------------
    try:
        if show_only_saved_local:
            saved_list = load_saved_bets()
        else:
            saved_list = []
    except Exception:
        saved_list = []

    if show_only_saved_local and saved_list:
        saved_df = pd.DataFrame(saved_list)

        saved_df = saved_df.rename(
            columns={
                "market_code": "market",
                "label": "bet_type",
                "book": "bookmaker",
            }
        )

        key_cols = ["player", "market", "line", "bet_type", "bookmaker"]

        if all(col in d.columns for col in key_cols) and all(
            col in saved_df.columns for col in key_cols
        ):
            d = d.merge(
                saved_df[key_cols].drop_duplicates(),
                on=key_cols,
                how="inner",
            )

    return d

# Pretty labels for game dropdown with team logos
game_pretty_labels = {}

for _, row in props_df.iterrows():
    home = row["home_team"]
    away = row["visitor_team"]
    key = f"{home} vs {away}"

    # Use your existing team logo dictionary
    home_logo = TEAM_LOGOS.get(TEAM_NAME_TO_CODE.get(home, ""), "")
    away_logo = TEAM_LOGOS.get(TEAM_NAME_TO_CODE.get(away, ""), "")


    game_pretty_labels[key] = f"🏀 {home} vs {away}"  # simple version

    # If you want logos:
    # game_pretty_labels[key] = f'<img src="{home_logo}" width="18"> {home} vs <img src="{away_logo}" width="18"> {away}'

# ------------------------------------------------------
# SHARED: RENDER SAVED BETS TAB (UNIVERSAL ACROSS SPORTS)
# ------------------------------------------------------
def render_saved_bets_tab():
    st.subheader("Saved Bets")

    if not st.session_state.saved_bets:
        st.info("No saved bets yet.")
        return

    # List saved bets
    for i, bet in enumerate(st.session_state.saved_bets):
        col1, col2 = st.columns([8, 1])

        with col1:
            st.markdown(
                f"""
                **{bet.get('player', '')}**  
                {bet.get('market', '')} **{bet.get('bet_type', '')} {bet.get('line', '')}**  
                Odds: **{bet.get('price', '')}** — Book: **{bet.get('bookmaker', '')}**
                """
            )
        with col2:
            if st.button("❌", key=f"remove_{i}"):
                st.session_state.saved_bets.pop(i)
                replace_saved_bets_in_db(user_id, st.session_state.saved_bets)
                st.rerun()

    st.write("---")

    if st.button("🗑️ Clear All Saved Bets"):
        st.session_state.saved_bets = []
        replace_saved_bets_in_db(user_id, [])
        st.success("All saved bets cleared.")
        st.rerun()

    st.write("---")

    txt_export = ""
    for b in st.session_state.saved_bets:
        txt_export += (
            f"{b.get('player', '')} | {b.get('market', '')} | "
            f"{b.get('bet_type', '')} {b.get('line', '')} | "
            f"Odds {b.get('price', '')} | {b.get('bookmaker', '')}\n"
        )

    st.download_button(
        "Download as Text",
        data=txt_export,
        file_name="saved_bets.txt",
        mime="text/plain",
    )

# ------------------------------------------------------
# TABS — NBA / NCAA + UNIVERSAL SAVED BETS
# ------------------------------------------------------

if sport == "NBA":
    # Saved Bets moved to LAST position in the bar
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab9, tab10, tab8 = st.tabs(
        [
            "📈 Props",
            "🏀 Game Lines",
            "🏅 EV Leaderboard",
            "🗺️ EV Heatmap",
            "📐 Trend Projection Model",
            "⏱️ Minutes & Usage",
            "📈 Trend Lab",
            "📋 Depth Chart & Injury Report",
            "🔀 WOWY Analyzer",
            "📋 Saved Bets",  # last in the bar
        ]
    )

    # ------------------------------------------------------
    # UNIFIED PROPS TAB (All Props + Filters + EV+)
    # ------------------------------------------------------
    with tab1:

        st.subheader("All Available Props (Full Slate)")

        # ------------------------------------------
        # EV+ FILTER SWITCH
        # ------------------------------------------
        show_ev_only = st.checkbox(
            "Show only EV+ bets (EV > Implied Probability)",
            value=False,
            help="When enabled, only props where the hit rate exceeds the implied probability will be shown."
        )

        # ------------------------------------------
        # SOURCE DF + BASIC NORMALIZATION
        # ------------------------------------------
        df = filter_props(props_df)

        # Ensure numeric
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["hit_rate_last5"] = pd.to_numeric(df["hit_rate_last5"], errors="coerce")
        df["hit_rate_last10"] = pd.to_numeric(df["hit_rate_last10"], errors="coerce")
        df["hit_rate_last20"] = pd.to_numeric(df["hit_rate_last20"], errors="coerce")

        # ------------------------------------------------------------
        # COLLAPSIBLE FILTER PANEL (This is your only filter UI)
        # ------------------------------------------------------------
        with st.expander("⚙️ Filters", expanded=False):

            st.markdown('<div class="filter-panel">', unsafe_allow_html=True)

            # Row 1 — Bet Type / Market / Games
            c1, c2, c3 = st.columns([1.2, 1.7, 1.5])

            with c1:
                f_bet_type = st.multiselect(
                    "Bet Type",
                    options=["Over", "Under"],
                    default=["Over", "Under"],
                )

            with c2:
                f_market = st.multiselect(
                    "Market",
                    options=market_list,
                    default=market_list,
                    format_func=lambda x: market_pretty_map.get(x, x)
                )

            with c3:
                f_games = st.multiselect(
                    "Games",
                    options=games_today,
                    default=games_today,
                    format_func=lambda x: x,             # Keep raw string
                )

            # Row 2 — Odds / Hit Window / % Hit Rate
            c4, c5, c6 = st.columns([1, 1, 1])

            with c4:
                f_min_odds = st.number_input("Min Odds", value=-600, step=10)
                f_max_odds = st.number_input("Max Odds", value=150, step=10)

            with c5:
                f_window = st.selectbox(
                    "Hit Window",
                    ["L5", "L10", "L20"],
                    index=1
                )

            with c6:
                f_min_hit = st.slider(
                    "Min Hit Rate (%)",
                    0, 100, 80
                )

            # Row 3 — Sportsbooks
            c7 = st.columns([1])[0]

            with c7:
                f_books = st.multiselect(
                    "Books",
                    options=sportsbook_list,
                    default=["DraftKings", "FanDuel"],   # NEW DEFAULT
                )

            st.markdown('</div>', unsafe_allow_html=True)

        # ------------------------------------------------------------
        # APPLY FILTERS TO DF (based on above panel)
        # ------------------------------------------------------------
        df = df[df["bet_type"].isin(f_bet_type)]
        df = df[df["market"].isin(f_market)]
        df = df[df["bookmaker"].isin(f_books)]
        df = df[(df["price"] >= f_min_odds) & (df["price"] <= f_max_odds)]

        # Map hit window
        window_col = {
            "L5": "hit_rate_last5",
            "L10": "hit_rate_last10",
            "L20": "hit_rate_last20",
        }[f_window]

        # Convert hit slider % → decimal
        hit_rate_decimal = f_min_hit / 100.0
        df = df[df[window_col] >= hit_rate_decimal]

        # Game filter — home vs away format
        df["game_display"] = (
            df["home_team"].astype(str)
            + " vs "
            + df["visitor_team"].astype(str)
        )
        df = df[df["game_display"].isin(f_games)]

        # ------------------------------------------------------------
        # EV+ ONLY FILTER (optional)
        # ------------------------------------------------------------
        if show_ev_only:
            def is_ev_plus(row):
                hit = row.get(window_col)
                implied = compute_implied_prob(row.get("price"))
                return (hit is not None) and (implied is not None) and (hit > implied)

            df = df[df.apply(is_ev_plus, axis=1)]

        # ------------------------------------------------------------
        # SORTING — Hit Rate DESC, then Odds ASC
        # ------------------------------------------------------------
        if window_col in df.columns:
            df = df.sort_values([window_col, "price"], ascending=[False, True])

        # ------------------------------------------------------------
        # CARD RENDERING
        # ------------------------------------------------------------
        render_prop_cards(
            df=df,
            require_ev_plus=False,
            odds_min=f_min_odds,
            odds_max=f_max_odds,
            min_hit_rate=hit_rate_decimal,
            hit_rate_col=window_col,
            hit_label=f_window,
            min_opp_rank=None,
            page_key="props_unified"
        )

        # ------------------------------------------------------
        # TAB 2 — GAME LINES + MODEL EV (Spread / Total / ML)
        # ------------------------------------------------------
        with tab2:
            st.subheader("Game Lines & Model EV (Spread · Total · Moneyline)")

            if game_report_df.empty:
                st.info("No game report data for today. Make sure nba_prop_analyzer.game_report is populated.")
                st.stop()

            # Work off a clean copy
            df = game_report_df.copy()

            # Ensure numeric types
            num_cols = [
                "home_team_strength", "visitor_team_strength",
                "predicted_margin",
                "home_win_pct", "visitor_win_pct",
                "exp_home_points", "exp_visitor_points", "exp_total_points",
                "pace_proxy", "pace_delta",
                "home_over_expected", "visitor_over_expected",
                "home_l5_diff", "visitor_l5_diff",
                "home_l10_diff", "visitor_l10_diff",
                "home_avg_pts_scored", "home_avg_pts_allowed",
                "visitor_avg_pts_scored", "visitor_avg_pts_allowed",
                "home_spread", "visitor_spread",
                "home_spread_price", "visitor_spread_price",
                "home_spread_edge", "visitor_spread_edge",
                "total_line", "total_price", "total_edge_pts",
                "home_ml", "visitor_ml",
                "home_implied_prob", "visitor_implied_prob",
                "home_ml_edge", "visitor_ml_edge",
            ]
            for c in num_cols:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            # Simple matchup label
            df["matchup"] = df["home_team"] + " vs " + df["visitor_team"]

            # --------------------------------------------------
            # 2A. TOP EDGES SUMMARY (Spread / Total / Moneyline)
            # --------------------------------------------------
            st.markdown("### 🔍 Top Edges (Model vs Book)")

            c_spread, c_total, c_ml = st.columns(3)

            # ---------------------
            # Spread edges
            # ---------------------
            spread_rows = []
            for _, r in df.iterrows():
                m = r["matchup"]

                # Home spread
                if pd.notna(r.home_spread_edge):
                    spread_rows.append({
                        "Matchup": m,
                        "Side": f"{r.home_team} (Home)",
                        "Spread": r.home_spread,
                        "Price": r.home_spread_price,
                        "Edge_pts": r.home_spread_edge,
                    })

                # Away spread
                if pd.notna(r.visitor_spread_edge):
                    spread_rows.append({
                        "Matchup": m,
                        "Side": f"{r.visitor_team} (Away)",
                        "Spread": r.visitor_spread,
                        "Price": r.visitor_spread_price,
                        "Edge_pts": r.visitor_spread_edge,
                    })

            if spread_rows:
                spread_df = pd.DataFrame(spread_rows)
                spread_df = spread_df.sort_values("Edge_pts", ascending=False).head(6)
                spread_df["Price"] = spread_df["Price"].apply(format_moneyline)
                spread_df["Edge_pts"] = spread_df["Edge_pts"].round(2)

                with c_spread:
                    st.markdown("**Spread Model (Edge in points)**")
                    st.dataframe(spread_df, use_container_width=True, hide_index=True)
            else:
                with c_spread:
                    st.info("No spread edge data in game_report.")

            # ---------------------
            # Total edges
            # ---------------------
            total_rows = []
            for _, r in df.iterrows():
                if pd.notna(r.total_edge_pts):
                    total_rows.append({
                        "Matchup": r["matchup"],
                        "Total_line": r["total_line"],
                        "Price": r["total_price"],
                        "Edge_pts": r["total_edge_pts"],
                    })

            if total_rows:
                total_df = pd.DataFrame(total_rows)
                total_df = total_df.sort_values("Edge_pts", ascending=False).head(6)
                total_df["Price"] = total_df["Price"].apply(format_moneyline)
                total_df["Edge_pts"] = total_df["Edge_pts"].round(2)

                with c_total:
                    st.markdown("**Total Model (Edge in points)**")
                    st.dataframe(total_df, use_container_width=True, hide_index=True)
            else:
                with c_total:
                    st.info("No total edge data found.")

            # ---------------------
            # Moneyline edges
            # ---------------------
            ml_rows = []
            for _, r in df.iterrows():
                m = r.matchup

                if pd.notna(r.home_ml_edge):
                    ml_rows.append({
                        "Matchup": m,
                        "Side": f"{r.home_team} (Home)",
                        "ML": r.home_ml,
                        "Edge_pct": r.home_ml_edge * 100,
                    })
                if pd.notna(r.visitor_ml_edge):
                    ml_rows.append({
                        "Matchup": m,
                        "Side": f"{r.visitor_team} (Away)",
                        "ML": r.visitor_ml,
                        "Edge_pct": r.visitor_ml_edge * 100,
                    })

            if ml_rows:
                ml_df = pd.DataFrame(ml_rows)
                ml_df = ml_df.sort_values("Edge_pct", ascending=False).head(6)
                ml_df["ML"] = ml_df["ML"].apply(format_moneyline)
                ml_df["Edge_pct"] = ml_df["Edge_pct"].round(1)

                with c_ml:
                    st.markdown("**Moneyline Model (Edge in %)**")
                    st.dataframe(ml_df, use_container_width=True, hide_index=True)
            else:
                with c_ml:
                    st.info("No moneyline edge data.")

            st.markdown("---")

            # --------------------------------------------------
            # 2B. GAME HEATMAP — Pace × Total × Strength
            # --------------------------------------------------
            st.markdown("### 🔥 Game Heatmap (Pace × Total × Strength)")

            heat_df = df.dropna(subset=["exp_total_points", "pace_proxy", "home_team_strength", "visitor_team_strength"]).copy()
            heat_df["strength_diff"] = heat_df["home_team_strength"] - heat_df["visitor_team_strength"]

            if heat_df.empty:
                st.info("Not enough data to build the heatmap.")
            else:
                fig_heat = go.Figure(
                    data=go.Scatter(
                        x=heat_df["exp_total_points"],
                        y=heat_df["pace_proxy"],
                        mode="markers+text",
                        text=heat_df["matchup"],
                        textposition="top center",
                        marker=dict(
                            size=14,
                            color=heat_df["strength_diff"],
                            colorscale="RdBu",
                            showscale=True,
                            colorbar=dict(title="Home Strength Edge (pts)"),
                        ),
                    )
                )
                fig_heat.update_layout(
                    xaxis_title="Model Expected Total",
                    yaxis_title="Pace Proxy",
                    template="plotly_dark",
                    height=450,
                )
                st.plotly_chart(fig_heat, use_container_width=True)

            st.markdown("---")

            # --------------------------------------------------
            # 2C. MATCHUP CARDS (HTML)
            # --------------------------------------------------
            st.markdown("### 📋 Matchup Cards (Spread / Total / ML + Team Form)")

            df_cards = df.sort_values("exp_total_points", ascending=False)

            for _, r in df_cards.iterrows():
                home = r.home_team
                away = r.visitor_team

                home_logo = TEAM_LOGOS.get(TEAM_NAME_TO_CODE.get(home, ""), "")
                away_logo = TEAM_LOGOS.get(TEAM_NAME_TO_CODE.get(away, ""), "")

                # Build card
                card_html = f"""
                <div class="prop-card">

                    <div class="prop-headline">
                        <div>
                            <div class="prop-player">
                                <img src="{home_logo}" width="26" style="vertical-align:middle;margin-right:6px;" />
                                {home}
                                <span style="color:#64748b;font-size:0.8rem;">vs</span>
                                <img src="{away_logo}" width="26" style="vertical-align:middle;margin:0 6px;" />
                                {away}
                            </div>

                            <div class="prop-market">
                                Model score: {r.exp_home_points:.1f} – {r.exp_visitor_points:.1f}
                                <span style="color:#9ca3af;">(Total {r.exp_total_points:.1f})</span>
                            </div>

                            <div class="prop-market">
                                Model win %: {r.home_win_pct:.1f}% | {r.visitor_win_pct:.1f}%
                            </div>
                        </div>

                        <div style="text-align:right;">
                            <div class="pill-book">
                                Spread: {home} {r.home_spread:+.1f} ({format_moneyline(r.home_spread_price)}) /
                                {away} {r.visitor_spread:+.1f} ({format_moneyline(r.visitor_spread_price)})
                            </div>

                            <div class="pill-book" style="margin-top:6px;">
                                Total: {r.total_line:.1f} ({format_moneyline(r.total_price)})
                            </div>

                            <div class="pill-book" style="margin-top:6px;">
                                ML: {home} {format_moneyline(r.home_ml)} /
                                {away} {format_moneyline(r.visitor_ml)}
                            </div>
                        </div>
                    </div>

                    <div class="prop-meta">
                        <div>
                            <div class="prop-meta-label">Spread Edge</div>
                            <div class="prop-meta-value">
                                Home {r.home_spread_edge:+.2f} | Away {r.visitor_spread_edge:+.2f}
                            </div>
                        </div>

                        <div>
                            <div class="prop-meta-label">Total Edge</div>
                            <div class="prop-meta-value">{r.total_edge_pts:+.2f}</div>
                        </div>

                        <div>
                            <div class="prop-meta-label">ML Edge</div>
                            <div class="prop-meta-value">
                                Home {(r.home_ml_edge*100):+.1f}% | Away {(r.visitor_ml_edge*100):+.1f}%
                            </div>
                        </div>
                    </div>

                    <div class="prop-meta" style="margin-top:0.6rem;">
                        <div>
                            <div class="prop-meta-label">Home Off / Def</div>
                            <div class="prop-meta-value">{r.home_avg_pts_scored:.1f} / {r.home_avg_pts_allowed:.1f}</div>
                        </div>

                        <div>
                            <div class="prop-meta-label">Away Off / Def</div>
                            <div class="prop-meta-value">{r.visitor_avg_pts_scored:.1f} / {r.visitor_avg_pts_allowed:.1f}</div>
                        </div>

                        <div>
                            <div class="prop-meta-label">Form (L10 Diff)</div>
                            <div class="prop-meta-value">
                                Home {r.home_l10_diff:+.1f} | Away {r.visitor_l10_diff:+.1f}
                            </div>
                        </div>
                    </div>

                </div>
                """

                st.markdown(card_html, unsafe_allow_html=True)

                # Trend mini-chart
                trend_df = pd.DataFrame(
                    {
                        "Team": [f"{home} Off", f"{home} Def", f"{away} Off", f"{away} Def"],
                        "Points": [
                            r.home_avg_pts_scored,
                            r.home_avg_pts_allowed,
                            r.visitor_avg_pts_scored,
                            r.visitor_avg_pts_allowed,
                        ],
                    }
                )

                fig_trend = go.Figure(go.Bar(x=trend_df["Team"], y=trend_df["Points"]))
                fig_trend.update_layout(
                    template="plotly_dark",
                    height=220,
                    margin=dict(l=40, r=20, t=10, b=60),
                    yaxis_title="Season Avg Points",
                    xaxis_tickangle=-30,
                )
                st.plotly_chart(fig_trend, use_container_width=True)

                st.markdown("---")



    # ------------------------------------------------------
    # TAB 3 — EV LEADERBOARD
    # ------------------------------------------------------
    with tab3:
        st.subheader("EV Leaderboard (Edge vs Market)")

        if props_df.empty:
            st.info("No props available for today.")
        else:
            df_leader = props_df.copy()

            # Make sure key numeric fields are numeric
            for c in ["edge_pct", "edge_raw", "ev_last10", "hit_rate_last10", "price"]:
                if c in df_leader.columns:
                    df_leader[c] = pd.to_numeric(df_leader[c], errors="coerce")

            # Simple filters
            col1, col2 = st.columns(2)
            with col1:
                min_edge = st.slider(
                    "Minimum Edge (%)",
                    min_value=-20,
                    max_value=50,
                    value=0,
                    step=1,
                )
            with col2:
                min_hit = st.slider(
                    "Minimum L10 Hit Rate (%)",
                    min_value=0,
                    max_value=100,
                    value=60,
                    step=5,
                )

            # Filter on edge + hit rate if columns exist
            if "edge_pct" in df_leader.columns:
                df_leader = df_leader[df_leader["edge_pct"] >= min_edge / 100.0]
            if "hit_rate_last10" in df_leader.columns:
                df_leader = df_leader[df_leader["hit_rate_last10"] >= min_hit / 100.0]

            # Sort: highest edge then highest EV
            sort_cols = [c for c in ["edge_pct", "ev_last10"] if c in df_leader.columns]
            if sort_cols:
                df_leader = df_leader.sort_values(sort_cols, ascending=False)

            # Pretty market name
            df_leader["market_pretty"] = df_leader["market"].map(
                lambda m: MARKET_DISPLAY_MAP.get(m, m)
            )

            cols_to_show = [
                "player",
                "market_pretty",
                "bet_type",
                "line",
                "price",
                "hit_rate_last10",
                "implied_prob",
                "edge_pct",
                "ev_last10",
                "proj_last10",
                "proj_diff_vs_line",
                "matchup_difficulty_score",
                "est_minutes",
                "usage_bump_pct",
            ]
            cols_to_show = [c for c in cols_to_show if c in df_leader.columns]

            if df_leader.empty:
                st.info("No props meet the current leaderboard filters.")
            else:
                display_df = df_leader[cols_to_show].copy()

                # Format a few columns
                if "price" in display_df.columns:
                    display_df["price"] = display_df["price"].apply(format_moneyline)

                if "hit_rate_last10" in display_df.columns:
                    display_df["hit_rate_last10"] = (display_df["hit_rate_last10"] * 100).round(1)

                if "implied_prob" in display_df.columns:
                    display_df["implied_prob"] = (display_df["implied_prob"] * 100).round(1)

                if "edge_pct" in display_df.columns:
                    display_df["edge_pct"] = display_df["edge_pct"].round(1)

                if "matchup_difficulty_score" in display_df.columns:
                    display_df["matchup_difficulty_score"] = display_df["matchup_difficulty_score"].round(1)

                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                )

    # ------------------------------------------------------
    # TAB 4 — DAILY EV HEATMAP
    # ------------------------------------------------------
    with tab4:
        st.subheader("Daily EV Heatmap by Stat vs Opponent")

        if props_df.empty:
            st.info("No props available for today.")
        else:
            heat_df = props_df.copy()

            # Derive a simple stat key from the market string (pts, reb, ast, pra, etc.)
            heat_df["stat_key"] = heat_df["market"].apply(detect_stat)

            if "edge_pct" not in heat_df.columns:
                st.warning("edge_pct column is missing; heatmap cannot be built.")
            else:
                heat_df["edge_pct"] = pd.to_numeric(heat_df["edge_pct"], errors="coerce")
                heat_df = heat_df[
                    heat_df["stat_key"].notna()
                    & (heat_df["stat_key"] != "")
                    & heat_df["opponent_team"].notna()
                    & heat_df["edge_pct"].notna()
                ]

                if heat_df.empty:
                    st.info("Not enough data to build the heatmap.")
                else:
                    pivot = heat_df.pivot_table(
                        index="stat_key",
                        columns="opponent_team",
                        values="edge_pct",
                        aggfunc="mean",
                    )

                    fig = go.Figure(
                        data=go.Heatmap(
                            z=pivot.values,
                            x=list(pivot.columns),
                            y=list(pivot.index),
                            colorscale="RdYlGn",
                            zmid=0,
                            colorbar=dict(title="Edge (%)"),
                        )
                    )
                    fig.update_layout(
                        template="plotly_dark",
                        height=500,
                        margin=dict(l=40, r=20, t=40, b=80),
                        xaxis_title="Opponent",
                        yaxis_title="Stat",
                    )

                    # Convert to % in hover
                    fig.update_traces(
                        hovertemplate="Stat: %{y}<br>Opponent: %{x}<br>Edge: %{z:.1f}%%<extra></extra>"
                    )

                    st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------
    # TAB 5 — TREND PROJECTION MODEL
    # ------------------------------------------------------
    with tab5:
        st.subheader("Trend-Based Projection Model")

        if props_df.empty:
            st.info("No props available for today.")
        else:
            proj_df = props_df.copy()

            needed = [
                "proj_last10",
                "proj_std_last10",
                "proj_volatility_index",
                "proj_diff_vs_line",
                "hit_rate_last10",
                "price",
            ]
            for c in needed:
                if c in proj_df.columns:
                    proj_df[c] = pd.to_numeric(proj_df[c], errors="coerce")

            # Filters
            c1, c2, c3 = st.columns(3)
            with c1:
                min_proj_diff = st.slider(
                    "Min Projection vs Line (points)",
                    min_value=-10.0,
                    max_value=20.0,
                    value=1.0,
                    step=0.5,
                )
            with c2:
                max_vol_index = st.slider(
                    "Max Volatility Index",
                    min_value=0.0,
                    max_value=5.0,
                    value=3.0,
                    step=0.1,
                )
            with c3:
                min_hit10_proj = st.slider(
                    "Min Hit Rate L10 (%)",
                    min_value=0,
                    max_value=100,
                    value=50,
                    step=5,
                )

            if "proj_diff_vs_line" in proj_df.columns:
                proj_df = proj_df[proj_df["proj_diff_vs_line"] >= min_proj_diff]

            if "proj_volatility_index" in proj_df.columns:
                proj_df = proj_df[proj_df["proj_volatility_index"] <= max_vol_index]

            if "hit_rate_last10" in proj_df.columns:
                proj_df = proj_df[proj_df["hit_rate_last10"] >= min_hit10_proj / 100.0]

            proj_df["market_pretty"] = proj_df["market"].map(
                lambda m: MARKET_DISPLAY_MAP.get(m, m)
            )

            cols = [
                "player",
                "market_pretty",
                "bet_type",
                "line",
                "price",
                "proj_last10",
                "proj_std_last10",
                "proj_volatility_index",
                "proj_diff_vs_line",
                "hit_rate_last10",
                "edge_pct",
            ]
            cols = [c for c in cols if c in proj_df.columns]

            if proj_df.empty:
                st.info("No props match the current projection filters.")
            else:
                proj_df = proj_df.sort_values(
                    by=[c for c in ["proj_diff_vs_line", "edge_pct"] if c in proj_df.columns],
                    ascending=False,
                )
                display_df = proj_df[cols].copy()

                if "price" in display_df.columns:
                    display_df["price"] = display_df["price"].apply(format_moneyline)
                if "hit_rate_last10" in display_df.columns:
                    display_df["hit_rate_last10"] = (display_df["hit_rate_last10"] * 100).round(1)
                if "edge_pct" in display_df.columns:
                    display_df["edge_pct"] = display_df["edge_pct"].round(1)

                st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ------------------------------------------------------
    # TAB 6 — MINUTES & USAGE MODEL
    # ------------------------------------------------------
    with tab6:
        st.subheader("Minutes & Usage (WOWY + Role Context)")

        if props_df.empty:
            st.info("No props available for today.")
        else:
            mu_df = props_df.copy()

            for c in ["est_minutes", "usage_bump_pct", "proj_diff_vs_line", "ev_last10"]:
                if c in mu_df.columns:
                    mu_df[c] = pd.to_numeric(mu_df[c], errors="coerce")

            c1, c2, c3 = st.columns(3)
            with c1:
                min_minutes = st.slider(
                    "Min Estimated Minutes",
                    min_value=0,
                    max_value=48,
                    value=24,
                    step=2,
                )
            with c2:
                min_usage_bump = st.slider(
                    "Min Usage Bump (%)",
                    min_value=-20,
                    max_value=60,
                    value=5,
                    step=1,
                )
            with c3:
                min_proj_diff_mu = st.slider(
                    "Min Projection vs Line (points)",
                    min_value=-10.0,
                    max_value=20.0,
                    value=0.0,
                    step=0.5,
                )

            if "est_minutes" in mu_df.columns:
                mu_df = mu_df[mu_df["est_minutes"] >= min_minutes]

            if "usage_bump_pct" in mu_df.columns:
                mu_df = mu_df[mu_df["usage_bump_pct"] >= min_usage_bump]

            if "proj_diff_vs_line" in mu_df.columns:
                mu_df = mu_df[mu_df["proj_diff_vs_line"] >= min_proj_diff_mu]

            mu_df["market_pretty"] = mu_df["market"].map(
                lambda m: MARKET_DISPLAY_MAP.get(m, m)
            )

            cols = [
                "player",
                "player_team",
                "market_pretty",
                "bet_type",
                "line",
                "price",
                "est_minutes",
                "usage_bump_pct",
                "proj_diff_vs_line",
                "ev_last10",
                "hit_rate_last10",
                "matchup_difficulty_score",
            ]
            cols = [c for c in cols if c in mu_df.columns]

            if mu_df.empty:
                st.info("No props match the current minutes/usage filters.")
            else:
                mu_df = mu_df.sort_values(
                    by=[c for c in ["usage_bump_pct", "est_minutes", "proj_diff_vs_line"] if c in mu_df.columns],
                    ascending=False,
                )
                display_df = mu_df[cols].copy()

                if "price" in display_df.columns:
                    display_df["price"] = display_df["price"].apply(format_moneyline)
                if "hit_rate_last10" in display_df.columns:
                    display_df["hit_rate_last10"] = (display_df["hit_rate_last10"] * 100).round(1)

                st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ------------------------------------------------------
    # TAB 7 — TREND LAB (same as your old Tab 2)
    # ------------------------------------------------------
    with tab7:
        st.subheader("Trend Lab (Real History + Dynamic Line)")

        c1, c2, c3 = st.columns([1.2, 1.2, 1])
        with c1:
            player = st.selectbox(
                "Player", sorted(props_df["player"].dropna().unique())
            )
        with c2:
            stat_label = st.selectbox("Stat", ["Points", "Rebounds", "Assists", "P+R+A", "Steals", "Blocks"])
        with c3:
            n_games = st.slider("Last N games", 5, 25, 15)

        stat_map = {
            "Points": "pts",
            "Rebounds": "reb",
            "Assists": "ast",
            "P+R+A": "pra",
            "Steals": "stl",
            "Blocks": "blk",
        }

        stat = stat_map[stat_label]

        def clean_name(name):
            if not isinstance(name, str):
                return ""
            return (
                name.lower()
                .replace(".", "")
                .replace("-", " ")
                .strip()
            )

        history_df["player_clean"] = history_df["player"].apply(clean_name)
        player_clean = clean_name(player)

        df_trend = history_df[history_df["player_clean"] == player_clean].copy()

        df_trend[stat] = pd.to_numeric(df_trend[stat], errors="coerce")
        df_trend = df_trend[df_trend[stat].notna()]

        df_trend = (
            df_trend.sort_values("game_date")
            .drop_duplicates(subset=["game_date"], keep="last")
            .reset_index(drop=True)
        )

        df_trend = df_trend.sort_values("game_date").tail(n_games).reset_index(drop=True)

        if df_trend.empty:
            st.info("No historical data found for this selection.")
        else:
            df_trend["date_str"] = df_trend["game_date"].dt.strftime("%b %d")

            market_map = {
                "Points": "player_points_alternate",
                "Rebounds": "player_rebounds_alternate",
                "Assists": "player_assists_alternate",
                "P+R+A": "player_points_rebounds_assists_alternate",
                "Steals": "player_steals_alternate",
                "Blocks": "player_blocks_alternate",
            }

            selected_market_code = market_map[stat_label]

            player_props = props_df[
                (props_df["player"] == player)
                & (props_df["market"] == selected_market_code)
            ]

            if not player_props.empty:
                available_lines = sorted(
                    player_props["line"].dropna().unique().astype(float)
                )
                line = st.selectbox("Line", available_lines, index=0)
            else:
                line = st.number_input(
                    f"No real props found. Enter custom line for {stat_label}",
                    min_value=0.0,
                    value=10.0,
                    step=0.5,
                )

            df_trend["hit"] = df_trend[stat] > float(line)
            df_trend["rolling"] = df_trend[stat].rolling(window=5, min_periods=1).mean()

            hit_rate = df_trend["hit"].mean()
            avg_last5 = df_trend[stat].tail(5).mean()
            std_dev = df_trend[stat].std()
            last_game_value = df_trend[stat].iloc[-1]

            metric_row = f"""
            <div class="metric-grid" style="margin-top:0.25rem;margin-bottom:1rem;">
                <div class="metric-card">
                    <div class="metric-label">Hit Rate</div>
                    <div class="metric-value">{hit_rate:.0%}</div>
                    <div class="metric-sub">{df_trend['hit'].sum()} of {len(df_trend)} games</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Avg Last 5</div>
                    <div class="metric-value">{avg_last5:.1f}</div>
                    <div class="metric-sub">recent form</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Consistency</div>
                    <div class="metric-value">{std_dev:.1f}</div>
                    <div class="metric-sub">std deviation</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Last Game</div>
                    <div class="metric-value">{last_game_value:.0f}</div>
                    <div class="metric-sub">{'Hit' if last_game_value > line else 'Miss'} vs {line}</div>
                </div>
            </div>
            """
            st.markdown(metric_row, unsafe_allow_html=True)

            hover = [
                (
                    f"<b>{row['date_str']}</b><br>"
                    f"{stat_label}: {row[stat]}<br>"
                    f"5-game avg: {row['rolling']:.1f}<br>"
                    f"Opponent: {row['opponent_team']}<br>"
                    f"{'Hit' if row['hit'] else 'Miss'} vs line {line}"
                )
                for _, row in df_trend.iterrows()
            ]

            fig = go.Figure()

            fig.add_bar(
                x=df_trend["date_str"],
                y=df_trend[stat],
                marker_color=["#22c55e" if h else "#ef4444" for h in df_trend["hit"]],
                hovertext=hover,
                hoverinfo="text",
                name="Game Result",
            )

            fig.add_trace(
                go.Scatter(
                    x=df_trend["date_str"],
                    y=df_trend["rolling"],
                    mode="lines+markers",
                    line=dict(width=3, color=theme["accent"]),
                    marker=dict(size=6),
                    name="5-game Avg",
                )
            )

            fig.add_hline(
                y=line,
                line_dash="dot",
                line_color="#e5e7eb",
                annotation_text=f"Line {line}",
                annotation_position="top left",
            )

            fig.update_layout(
                template="plotly_dark",
                height=420,
                margin=dict(l=30, r=20, t=40, b=30),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                bargap=0.25,
            )

            st.plotly_chart(fig, use_container_width=True)

            ribbon_html = "<div style='display:flex;gap:4px;margin:6px 0;'>"
            for h in df_trend["hit"]:
                ribbon_html += (
                    "<div style='width:14px;height:14px;border-radius:3px;background:#22c55e;'></div>"
                    if h
                    else "<div style='width:14px;height:14px;border-radius:3px;background:#ef4444;'></div>"
                )
            ribbon_html += "</div>"
            st.markdown(ribbon_html, unsafe_allow_html=True)

            table_df = df_trend.copy()
            table_df["Outcome"] = table_df["hit"].map({True: "Hit", False: "Miss"})
            table_df_display = table_df[["date_str", "opponent_team", stat, "Outcome"]]
            table_df_display.columns = ["Date", "Opponent", stat_label, "Outcome"]

            st.dataframe(table_df_display, use_container_width=True, hide_index=True)

    #-------------------------------------------------
    # TAB 9 — DEPTH CHART & INJURY REPORT
    #-------------------------------------------------
    with tab9:
        st.subheader("")

        # --------------------------------------------------------
        # GLOBAL CSS — SPACIOUS CARD GRID + INJURY BADGE SUPPORT
        # --------------------------------------------------------
        st.markdown("""
<style>

.depth-card {
    padding:18px 20px;
    margin-bottom:20px;
    border-radius:20px;
    border:1px solid rgba(148,163,184,0.35);
    background: radial-gradient(circle at top left, rgba(30,41,59,1), rgba(15,23,42,0.92));
    box-shadow: 0 22px 55px rgba(15,23,42,0.90);
    transition: transform .18s ease-out, box-shadow .18s ease-out, border-color .18s ease-out;
}

.depth-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 30px 70px rgba(15,23,42,1);
    border-color: #3b82f6;
}

.role-pill {
    font-size: 0.70rem;
    padding: 4px 10px;
    border-radius: 999px;
    display: inline-block;
    font-weight: 600;
    color: white;
    margin-top: 6px;
}

.injury-badge {
    padding: 3px 10px;
    border-radius: 999px;
    font-size: 0.68rem;
    font-weight: 700;
    color: white;
    margin-left: 6px;
}

.injury-card {
    padding:18px 20px;
    margin-bottom:22px;
    border-radius:20px;
    border:1px solid rgba(148,163,184,0.28);
    background: radial-gradient(circle at 0 0, rgba(42,0,0,0.85), rgba(40,0,0,0.65));
    box-shadow: 0 22px 55px rgba(15,23,42,0.95);
    transition: transform .18s ease-out, box-shadow .18s ease-out;
}

.injury-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 32px 75px rgba(15,23,42,1);
}

.header-flex {
    display: flex;
    align-items: center;
    gap: 14px;
    margin-bottom: 1.6rem;
}

.position-header {
    font-size: 1.2rem;
    font-weight: 700;
    color: #e5e7eb;
    margin-bottom: 10px;
    margin-top: 10px;
}

</style>
""", unsafe_allow_html=True)

        # ----------------------------
        # TEAM SELECTOR
        # ----------------------------
        teams_meta = (
            depth_df[["team_number", "team_abbr", "team_name"]]
            .drop_duplicates()
            .sort_values("team_name")
        )

        team_labels = [f"{r.team_name} ({r.team_abbr})" for r in teams_meta.itertuples()]
        label_to_meta = {label: row for label, row in zip(team_labels, teams_meta.itertuples())}

        selected_label = st.selectbox("Select Team", team_labels)
        team_row = label_to_meta[selected_label]

        selected_team_number = int(team_row.team_number)
        selected_abbr = team_row.team_abbr
        selected_name = team_row.team_name

        # Filter for selected team
        team_depth = depth_df[depth_df["team_number"] == selected_team_number].copy()
        team_injuries = injury_df[injury_df["team_abbrev"] == selected_abbr].copy()

        # ----------------------------
        # TEAM HEADER
        # ----------------------------
        logo = TEAM_LOGOS_BASE64.get(selected_abbr, "")
        components.html(
            f"<div class='header-flex'>"
            f"<img src='{logo}' style='height:55px;border-radius:12px;'/>"
            f"<div>"
            f"<div style='font-size:1.55rem;font-weight:700;color:#e5e7eb;'>{selected_name}</div>"
            f"<div style='font-size:0.9rem;color:#9ca3af;'>Depth chart & injury status</div>"
            f"</div></div>",
            height=90,
            scrolling=False,
        )

        col_left, col_right = st.columns([1.6, 1.0])

        # ------------------------------------------------------
        # DEPTH CHART (LEFT)
        # ------------------------------------------------------
        with col_left:
            st.markdown("## 🏀 Depth Chart")

            pos_order = ["PG", "SG", "SF", "PF", "C", "G", "F"]
            positions = sorted(
                team_depth["position"].unique(),
                key=lambda p: pos_order.index(p) if p in pos_order else 99
            )

            pos_cols = st.columns(min(3, len(positions)))

            for i, pos in enumerate(positions):
                with pos_cols[i % len(pos_cols)]:

                    st.markdown(f"<div class='position-header'>{pos}</div>", unsafe_allow_html=True)

                    rows = team_depth[team_depth["position"] == pos].sort_values("depth")

                    for _, r in rows.iterrows():
                        name = r["player"]
                        role = r["role"]
                        depth_val = r["depth"]
                        player_id = r.get("player_id", None)

                        # --------------------
                        # Lookup injury status
                        # --------------------
                        inj_status = None
                        injury_html = ""

                        if not team_injuries.empty:
                            # build quick lookup dict by NORMALIZED name
                            norm_name = name.lower().replace("'", "").replace(".", "").replace("-", "").strip()

                            for _, ir in team_injuries.iterrows():
                                n2 = f"{ir['first_name']} {ir['last_name']}".lower().replace("'", "").replace(".", "").replace("-", "").strip()
                                if n2 == norm_name:
                                    inj_status = ir.get("status")
                                    break

                        if inj_status:
                            s = inj_status.lower()
                            if "out" in s:
                                badge_color = "background:#ef4444;"
                            elif "question" in s or "doubt" in s:
                                badge_color = "background:#eab308;"
                            else:
                                badge_color = "background:#3b82f6;"
                            injury_html = f"<span class='injury-badge' style='{badge_color}'>{inj_status.upper()}</span>"

                        # Role color
                        rl = role.lower()
                        if rl.startswith("start"):
                            role_color = "background:#22c55e;"
                        elif "rotation" in rl:
                            role_color = "background:#3b82f6;"
                        else:
                            role_color = "background:#6b7280;"

                        html = (
                            f"<div class='depth-card'>"
                            f"  <div style='display:flex;justify-content:space-between;align-items:center;'>"
                            f"    <div>"
                            f"      <div style='font-size:1.05rem;font-weight:700;color:white;'>{name}{injury_html}</div>"
                            f"      <span class='role-pill' style='{role_color}'>{role}</span>"
                            f"    </div>"
                            f"    <div style='font-size:0.8rem;color:#e5e7eb;"
                            f"          background:rgba(255,255,255,0.08);padding:5px 12px;border-radius:10px;"
                            f"          border:1px solid rgba(255,255,255,0.12);'>"
                            f"      Depth {depth_val}"
                            f"    </div>"
                            f"  </div>"
                            f"</div>"
                        )

                        components.html(html, height=110, scrolling=False)

        # ------------------------------------------------------
        # INJURY REPORT (RIGHT)
        # ------------------------------------------------------
        def make_injury_key(first, last):
            if not first:
                first = ""
            if not last:
                last = ""

            f = (
                str(first).lower()
                .replace("'", "")
                .replace(".", "")
                .replace("-", "")
                .strip()
            )
            l = (
                str(last).lower()
                .replace("'", "")
                .replace(".", "")
                .replace("-", "")
                .strip()
            )

            if not f and not l:
                return None

            return f"{f[:1]}-{l}"

        with col_right:
            st.markdown("## 🏥 Injury Report")

            if team_injuries.empty:
                st.success("No reported injuries.")
            else:
                last_ts = team_injuries["snapshot_ts"].max()
                st.caption(f"Last update: {last_ts.strftime('%b %d, %Y %I:%M %p')}")

                # ----------------------------------------
                # Create grouping key for reliable dedup
                # ----------------------------------------
                team_injuries["inj_key"] = team_injuries.apply(
                    lambda r: make_injury_key(r.get("first_name"), r.get("last_name")),
                    axis=1
                )

                latest = (
                    team_injuries
                    .sort_values("snapshot_ts")
                    .groupby("inj_key")
                    .tail(1)
                    .sort_values("status", ascending=True)
                )

                for _, r in latest.iterrows():

                    name     = f"{r['first_name']} {r['last_name']}"
                    status   = r.get("status", "Unknown")
                    return_date = r.get("return_date_raw", "N/A")

                    injury_type     = r.get("injury_type", "")
                    injury_location = r.get("injury_location", "")
                    injury_side     = r.get("injury_side", "")
                    injury_detail   = r.get("injury_detail", "")

                    short_comment = r.get("short_comment", "")
                    long_comment  = r.get("long_comment", "")

                    s = status.lower()
                    if "out" in s:
                        status_color = "background:#ef4444;"
                    elif "question" in s or "doubt" in s:
                        status_color = "background:#eab308;"
                    elif "prob" in s:
                        status_color = "background:#3b82f6;"
                    else:
                        status_color = "background:#6b7280;"

                    injury_parts = [
                        injury_type,
                        injury_location,
                        injury_side,
                        injury_detail,
                    ]
                    injury_line = " • ".join([p for p in injury_parts if p]) or "No injury detail provided."

                    html = f"""
                        <div class='injury-card'>
                            <div style='display:flex;justify-content:space-between;'>
                                <div style='font-size:1.05rem;font-weight:600;color:white;'>{name}</div>
                                <div class='injury-badge' style='{status_color}'>{status.upper()}</div>
                            </div>

                            <div style='font-size:0.85rem;color:#e5e7eb;margin-top:6px;'>
                                <b>Return:</b> {return_date}
                            </div>

                            <div style='font-size:0.85rem;color:#e5e7eb;margin-top:6px;'>
                                <b>Injury:</b> {injury_line}
                            </div>

                            <div style='font-size:0.85rem;color:#e5e7eb;margin-top:8px;'>
                                {short_comment}
                            </div>

                            <div style='font-size:0.8rem;color:#9ca3af;margin-top:6px;'>
                                {long_comment}
                            </div>
                        </div>
                    """

                    components.html(html, height=200, scrolling=False)

    # ------------------------------------------------------
    # TAB 10 — WOWY ANALYZER (your old Tab 5)
    # ------------------------------------------------------
    with tab10:
        st.subheader("🔀 WOWY (With/Without You) Analyzer")

        st.markdown("""
        Below is the full WOWY table — showing how each player's production
        changes when a specific teammate is **OUT**.
        
        Sort any column to explore the biggest deltas.
        """)

        wow = wowy_df.copy()
        wow = wow.sort_values("pts_delta", ascending=False)

        disp = wow[[
            "player_a",
            "team_abbr",
            "breakdown",
            "pts_delta",
            "reb_delta",
            "ast_delta",
            "pra_delta",
            "pts_reb_delta"
        ]]

        st.dataframe(
            disp,
            hide_index=True,
            use_container_width=True
        )

    # ------------------------------------------------------
    # TAB 8 — SAVED BETS (same logic as your old Tab 3)
    # ------------------------------------------------------
    with tab8:
        st.subheader("Saved Bets")

        if not st.session_state.saved_bets:
            st.info("No saved bets yet.")
        else:
            for i, bet in enumerate(st.session_state.saved_bets):
                col1, col2 = st.columns([8, 1])

                with col1:
                    st.markdown(
                        f"""
                        **{bet['player']}**  
                        {bet['market']} **{bet['bet_type']} {bet['line']}**  
                        Odds: **{bet['price']}** — Book: **{bet['bookmaker']}**
                        """
                    )
                with col2:
                    if st.button("❌", key=f"remove_{i}"):
                        st.session_state.saved_bets.pop(i)
                        replace_saved_bets_in_db(user_id, st.session_state.saved_bets)
                        st.rerun()

            st.write("---")

            if st.button("🗑️ Clear All Saved Bets"):
                st.session_state.saved_bets = []
                replace_saved_bets_in_db(user_id, [])
                st.success("All saved bets cleared.")
                st.rerun()

            st.write("---")

            txt_export = ""
            for b in st.session_state.saved_bets:
                txt_export += (
                    f"{b['player']} | {b['market']} | {b['bet_type']} {b['line']} | "
                    f"Odds {b['price']} | {b['bookmaker']}\n"
                )

            st.download_button(
                "Download as Text",
                data=txt_export,
                file_name="saved_bets.txt",
                mime="text/plain",
            )

# ------------------------------------------------------
# NCAA MEN'S / WOMEN'S — Placeholder Tabs
# ------------------------------------------------------
elif sport in ["NCAA Men's", "NCAA Women's"]:

    tabN1, tabN2, tabN3, tabN4, tabN5 = st.tabs(
        [
            "📈 Props",
            "📊 Team Stats",
            "📅 Game Logs",
            "📋 Injury Report",
            "📋 Saved Bets",
        ]
    )

    with tabN1:
        st.subheader(f"{sport} Props")
        st.info(f"{sport} props coming soon.")

    with tabN2:
        st.subheader(f"{sport} Team Stats")
        st.info(f"{sport} team stats view coming soon.")

    with tabN3:
        st.subheader(f"{sport} Game Logs")
        st.info(f"{sport} game logs coming soon.")

    with tabN4:
        st.subheader(f"{sport} Injury Report")
        st.info(f"{sport} injury data coming soon.")

    with tabN5:
        render_saved_bets_tab()

# ------------------------------------------------------
# LAST UPDATED FOOTER
# ------------------------------------------------------
now = datetime.now(EST)
st.sidebar.markdown(f"**Last Updated:** {now.strftime('%b %d, %I:%M %p')} ET")

# ------------------------------------------------------
# LAST UPDATED FOOTER
# ------------------------------------------------------
now = datetime.now(EST)
st.sidebar.markdown(f"**Last Updated:** {now.strftime('%b %d, %I:%M %p')} ET")
