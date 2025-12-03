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

  ARRAY(SELECT x FROM UNNEST(pts_last5_list) x) AS pts_last5_list,
  ARRAY(SELECT x FROM UNNEST(reb_last5_list) x) AS reb_last5_list,
  ARRAY(SELECT x FROM UNNEST(ast_last5_list) x) AS ast_last5_list,
  ARRAY(SELECT x FROM UNNEST(stl_last5_list) x) AS stl_last5_list,
  ARRAY(SELECT x FROM UNNEST(blk_last5_list) x) AS blk_last5_list,
  ARRAY(SELECT x FROM UNNEST(pra_last5_list) x) AS pra_last5_list,

  ARRAY(SELECT x FROM UNNEST(pts_last7_list) x) AS pts_last7_list,
  ARRAY(SELECT x FROM UNNEST(reb_last7_list) x) AS reb_last7_list,
  ARRAY(SELECT x FROM UNNEST(ast_last7_list) x) AS ast_last7_list,
  ARRAY(SELECT x FROM UNNEST(stl_last7_list) x) AS stl_last7_list,
  ARRAY(SELECT x FROM UNNEST(blk_last7_list) x) AS blk_last7_list,
  ARRAY(SELECT x FROM UNNEST(pra_last7_list) x) AS pra_last7_list,

  ARRAY(SELECT x FROM UNNEST(pts_last10_list) x) AS pts_last10_list,
  ARRAY(SELECT x FROM UNNEST(reb_last10_list) x) AS reb_last10_list,
  ARRAY(SELECT x FROM UNNEST(ast_last10_list) x) AS ast_last10_list,
  ARRAY(SELECT x FROM UNNEST(stl_last10_list) x) AS stl_last10_list,
  ARRAY(SELECT x FROM UNNEST(blk_last10_list) x) AS blk_last10_list,
  ARRAY(SELECT x FROM UNNEST(pra_last10_list) x) AS pra_last10_list

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
  player_id,
  first_name,
  last_name,
  current_team,
  team_abbrev,
  status,
  return_date_raw,
  description
FROM {PROJECT_ID}.nba_prop_analyzer.player_injuries_raw
ORDER BY snapshot_ts DESC
"""

# NEW: WOWY delta SQL
DELTA_SQL = f"""
SELECT *
FROM {PROJECT_ID}.nba_prop_analyzer.player_wowy_deltas
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
# GLOBAL STYLES (from dev)
# ------------------------------------------------------
st.markdown(
    f"""
    <style>
    html, body, [class*="css"] {{
        font-family: "Inter", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}

    .block-container {{
        padding-top: 1rem !important;
        padding-bottom: 2rem !important;
        max-width: 1400px !important;
    }}

    body {{
        background: radial-gradient(circle at top, {theme["bg"]} 0, #000 55%) !important;
    }}

    [data-testid="stSidebar"] {{
        background: radial-gradient(circle at top left, #1f2937 0, #020617 55%);
        border-right: 1px solid rgba(255,255,255,0.04);
    }}

    [data-testid="stSidebar"] * {{
        color: #e5e7eb !important;
    }}

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
        margin: 0;
        letter-spacing: 0.02em;
        color: #e5e7eb;
    }}

    .app-subtitle {{
        font-size: 0.78rem;
        margin: 0;
        color: #9ca3af;
    }}

    .pill {{
        padding: 4px 12px;
        border-radius: 999px;
        border: 1px solid rgba(148,163,184,0.4);
        font-size: 0.7rem;
        text-transform: uppercase;
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
        transition: transform 0.14s ease-out, box-shadow 0.14s ease-out, border-color 0.14s ease-out;
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
        margin-bottom: 0.15rem;
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

    .prop-card {{
        border-radius: 16px;
        padding: 0.75rem 0.9rem;
        border: 1px solid rgba(148,163,184,0.28);
        background: radial-gradient(circle at 0 0, rgba(15,23,42,1), rgba(15,23,42,0.96));
        box-shadow: 0 20px 50px rgba(15,23,42,0.95);
        margin-bottom: 0.9rem;
        transition: transform 0.16s ease-out, box-shadow 0.16s ease-out, border-color 0.16s ease-out;
    }}

    .prop-card:hover {{
        transform: translateY(-3px);
        box-shadow: 0 26px 60px rgba(15,23,42,1);
        border-color: {theme["accent"]};
    }}

    .prop-headline {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.35rem;
    }}

    .prop-player {{
        font-weight: 600;
        font-size: 0.92rem;
        color: #e5e7eb;
    }}

    .prop-market {{
        font-size: 0.78rem;
        color: #9ca3af;
    }}

    .pill-book {{
        padding: 2px 8px;
        border-radius: 999px;
        font-size: 0.7rem;
        border: 1px solid rgba(148,163,184,0.4);
        color: #e5e7eb;
    }}

    .prop-meta {{
        display: flex;
        justify-content: space-between;
        gap: 0.5rem;
        font-size: 0.75rem;
        color: #9ca3af;
    }}

    .stDataFrame, .stDataEditor,
    [data-testid="stDataFrame"] > div,
    [data-testid="stDataEditor"] > div {{
        border-radius: 16px !important;
        box-shadow: 0 20px 50px rgba(15,23,42,0.98) !important;
        border: 1px solid rgba(148,163,184,0.45) !important;
        overflow: hidden;
        background: radial-gradient(circle at top left, rgba(15,23,42,0.98), rgba(15,23,42,0.96));
    }}

    .stDataFrame table, .stDataEditor table,
    [data-testid="stDataFrame"] table,
    [data-testid="stDataEditor"] table {{
        width: 100%;
        border-collapse: collapse;
    }}

    .stDataFrame table td, .stDataFrame table th,
    .stDataEditor table td, .stDataEditor table th,
    [data-testid="stDataFrame"] table td, [data-testid="stDataFrame"] table th,
    [data-testid="stDataEditor"] table td, [data-testid="stDataEditor"] table th {{
        text-align: center !important;
        vertical-align: middle !important;
    }}

    .stDataFrame thead th, .stDataEditor thead th,
    [data-testid="stDataFrame"] thead th,
    [data-testid="stDataEditor"] thead th {{
        background: #020617 !important;
        color: #e5e7eb !important;
        font-weight: 700 !important;
        font-size: 0.78rem !important;
        border-bottom: 1px solid rgba(148,163,184,0.45) !important;
    }}

    .stDataFrame tbody tr:nth-child(even) td,
    .stDataEditor tbody tr:nth-child(even) td,
    [data-testid="stDataFrame"] tbody tr:nth-child(even) td,
    [data-testid="stDataEditor"] tbody tr:nth-child(even) td {{
        background-color: rgba(17,24,39,0.9) !important;
    }}

    .stDataFrame tbody tr:nth-child(odd) td,
    .stDataEditor tbody tr:nth-child(odd) td,
    [data-testid="stDataFrame"] tbody tr:nth-child(odd) td,
    [data-testid="stDataEditor"] tbody tr:nth-child(odd) td {{
        background-color: rgba(15,23,42,0.95) !important;
    }}

    .stDataFrame tbody tr:hover td,
    .stDataEditor tbody tr:hover td,
    [data-testid="stDataFrame"] tbody tr:hover td,
    [data-testid="stDataEditor"] tbody tr:hover td {{
        background-color: rgba(15,23,42,1) !important;
    }}

    .stDataFrame tbody td,
    .stDataEditor tbody td,
    [data-testid="stDataFrame"] tbody td,
    [data-testid="stDataEditor"] tbody td {{
        font-size: 0.8rem !important;
        border-bottom: 1px solid rgba(31,41,55,0.85) !important;
    }}

    .stButton > button {{
        border-radius: 999px !important;
        padding: 0.35rem 0.95rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.03em;
        border: 1px solid rgba(148,163,184,0.4) !important;
        background: radial-gradient(circle at 0 0, {theme["accent"]}, {theme["accent_soft"]} 50%, #020617 100%);
        color: #f9fafb !important;
        box-shadow: 0 12px 30px rgba(8,47,73,0.9);
        transition: all 0.16s ease-out !important;
    }}

    .stButton > button:hover {{
        transform: translateY(-1px) scale(1.01);
        box-shadow: 0 16px 40px rgba(8,47,73,1);
    }}

    button[data-baseweb="tab"],
    [data-testid="stTabs"] button {{
        font-size: 0.8rem !important;
        text-transform: none !important;
    }}

    .stSidebar label,
    section[data-testid="stSidebar"] label {{
        font-size: 0.78rem !important;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #e5e7eb !important;
    }}

    .sparkline {{
        stroke: {theme["accent"]};
        fill: none;
        stroke-width: 2;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown("""
<style>

    /* ----------------------------------------------
       MOBILE FIX FOR AG-GRID (balham theme)
       ---------------------------------------------- */

    /* Force real width so columns don't collapse */
    .ag-theme-balham .ag-center-cols-container {
        min-width: 1100px !important;
    }

    /* Ensure horizontal scrolling works on mobile */
    .ag-theme-balham .ag-body-viewport,
    .ag-theme-balham .ag-center-cols-viewport,
    .ag-theme-balham .ag-root-wrapper,
    .ag-theme-balham .ag-root {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch !important;
    }

    /* Prevent cells & headers from shrinking too small */
    .ag-theme-balham .ag-header-cell,
    .ag-theme-balham .ag-cell {
        min-width: 115px !important;
        white-space: nowrap !important;
    }

</style>
""", unsafe_allow_html=True)


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

APP_ROOT = os.getcwd()

SPORTSBOOK_LOGOS = {
    "DraftKings": os.path.join(APP_ROOT, "static/logos/Draftkingssmall.png"),
    "FanDuel": os.path.join(APP_ROOT, "static/logos/Fanduelsmall.png"),
}

st.write("File exists:", os.path.exists(os.path.join(APP_ROOT, "static/logos/Draftkingssmall.png")))
st.write("Working directory:", APP_ROOT)

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
    "player_3pt_made_alternate": "3PT Made",
}

def build_tags_html(tags):
    return "".join(
        f'''
        <span style="
            background:{color};
            padding:3px 8px;
            border-radius:8px;
            margin-right:4px;
            font-size:0.68rem;
            font-weight:600;
            color:white;
            display:inline-block;
        ">{label}</span>
        '''
        for label, color in tags
    )

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
    if "stl" in m or "steal" in m:
        return "stl"
    if "blk" in m or "block" in m:
        return "blk"
    if "3" in m and ("fg3" in m or "3pt" in m or "three" in m):
        return "fg3m"

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
        # fallback for older schema
        df["matchup_difficulty_score"] = pd.to_numeric(
            df.get("matchup_difficulty_score"), errors="coerce"
        )

    for c in ["ev_last5", "ev_last10", "ev_last20"]:
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

# ------------------------------------------------------
# LOAD BASE TABLES
# ------------------------------------------------------
props_df = load_props()
history_df = load_history()

st.write("DEBUG: PROJECT_ID =", PROJECT_ID)
st.write("DEBUG: DATASET =", DATASET)
st.write("DEBUG: TABLE =", HISTORICAL_TABLE)

# Run one direct query:
test_df = bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{HISTORICAL_TABLE}` LIMIT 5").to_dataframe()
st.write("DEBUG: DIRECT QUERY SAMPLE:", test_df)
st.write("DEBUG: History after load + convert_list_columns")
st.write(history_df.head(10)[[
    "player",
    "pts_last5_list",
    "pts_last7_list",
    "pts_last10_list"
]])

depth_df = load_depth_charts()
injury_df = load_injury_report()    # <-- MUST COME BEFORE FIX
wowy_df = load_wowy_deltas()

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

st.write("DEBUG history_df sample WITH player_norm:")
st.write(history_df[[
    "player", "player_norm",
    "pts_last5_list", "pts_last7_list", "pts_last10_list"
]].head(20))


# ------------------------------------------------------
# ATTACH LAST-5 / LAST-7 / LAST-10 ARRAYS
# select MOST RECENT game per player (correct!)
# ------------------------------------------------------
hist_latest = (
    history_df.sort_values("game_date", ascending=False)
    .groupby("player_norm")
    .head(1)[[
        "player_norm",
        "pts_last5_list", "pts_last7_list", "pts_last10_list",
        "reb_last5_list", "reb_last7_list", "reb_last10_list",
        "ast_last5_list", "ast_last7_list", "ast_last10_list",
        "stl_last5_list", "stl_last7_list", "stl_last10_list",
        "blk_last5_list", "blk_last7_list", "blk_last10_list",
        "pra_last5_list", "pra_last7_list", "pra_last10_list",
    ]]
)

st.write("DEBUG hist_latest (should contain arrays):")
st.write(hist_latest.head(20))

# ------------------------------------------------------
# MERGE (now works because player_norm matches)
# ------------------------------------------------------
props_df = props_df.merge(hist_latest, on="player_norm", how="left")


# ------------------------------------------------------
# DEBUG — sanity check spark lists
# ------------------------------------------------------
st.write("DEBUG merged player lists (should NOT be empty):")
st.write(
    props_df[
        ["player", "player_norm", "pts_last5_list", "pts_last7_list", "pts_last10_list"]
    ].head(25)
)
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
    lambda x: x.split(" ")[1] if len(x.split(" ")) > 1 else ""
)

# Merge NEW: join by team_abbrev from injury table
merged = injury_df.merge(
    depth_df,
    left_on=["team_abbrev", "last_clean"],
    right_on=["team_abbr", "last_clean"],
    how="left",
    suffixes=("", "_roster")
)

# Verify first initial matches
def row_matches(row):
    inj_initial = row["first_clean"][0] if row["first_clean"] else ""
    roster_initial = row.get("first_initial", "")
    return inj_initial == roster_initial

merged["name_match"] = merged.apply(row_matches, axis=1)

# Keep only matched rows
injury_df = merged[merged["name_match"] == True].copy()

# Final clean columns
injury_df = injury_df[
    [
        "snapshot_ts", "player_id", "first_name", "last_name",
        "team_abbrev", "current_team",
        "status", "return_date_raw", "description",
        "team_number", "team_abbr", "team_name"
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
# SIDEBAR FILTERS (using production-style filters)
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

od_min = int(props_df["price"].min()) if not props_df.empty else -300
od_max = int(props_df["price"].max()) if not props_df.empty else 300
sel_odds = st.sidebar.slider("Odds Range", od_min, od_max, (od_min, od_max))

sel_hit10 = st.sidebar.slider("Min Hit Rate L10", 0.0, 1.0, 0.5)

show_only_saved = st.sidebar.checkbox("Show Only Saved Props", value=False)

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

manual_odds_min = st.sidebar.number_input(
    "Minimum Odds",
    value=-200,      # Default
    step=5
)

manual_odds_max = st.sidebar.number_input(
    "Maximum Odds",
    value=400,       # Default
    step=5
)

manual_l10_min = st.sidebar.number_input(
    "Minimum L10 Hit Rate (%)",
    min_value=0,
    max_value=100,
    value=80,        # Default: 80%
    step=1
)

# ------------------------------------------------------
# FILTER FUNCTION
# ------------------------------------------------------
def filter_props(df):
    d = df.copy()

    # Ensure numeric
    d["price"] = pd.to_numeric(d["price"], errors="coerce")
    d["hit_rate_last10"] = pd.to_numeric(d["hit_rate_last10"], errors="coerce")

    # ----- Sidebar filters -----
    if sel_game != "All games":
        home, away = sel_game.split(" vs ")
        d = d[(d["home_team"] == home) & (d["visitor_team"] == away)]

    if sel_player != "All players":
        d = d[d["player"] == sel_player]

    if sel_market != "All Stats":
        d = d[d["market"] == sel_market]

    if sel_books:
        d = d[d["bookmaker"].isin(sel_books)]

    # Odds slider
    d = d[d["price"].between(sel_odds[0], sel_odds[1])]

    # Global Min L10 Hit Rate
    d = d[d["hit_rate_last10"] >= sel_hit10]

    # Saved bets filter
    if show_only_saved and st.session_state.saved_bets:
        saved_df = pd.DataFrame(st.session_state.saved_bets)
        key_cols = ["player", "market", "line", "bet_type", "bookmaker"]
        if all(col in d.columns for col in key_cols) and all(
            col in saved_df.columns for col in key_cols
        ):
            d = d.merge(saved_df[key_cols], on=key_cols, how="inner")

    return d


# ------------------------------------------------------
# TABS
# ------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "🧮 Props Overview",
        "📈 Trend Lab",
        "📋 Saved Bets",
        "📋 Depth Chart & Injury Report",
        "🔀 WOWY Analyzer",
    ]
)

# ------------------------------------------------------
# TAB 1 — PROPS OVERVIEW (Card Grid + Advanced Table)
# ------------------------------------------------------
with tab1:

    st.subheader("Props Overview (Real Slate)")

    # Apply global sidebar filters
    filtered_df = filter_props(props_df)

    # ----------- TOP METRICS ----------
    total_props = len(filtered_df)
    avg_hit = filtered_df["hit_rate_last10"].mean() if total_props else 0
    avg_odds = filtered_df["price"].mean() if total_props else 0
    avg_matchup = filtered_df["matchup_difficulty_score"].mean() if total_props else 0

    metrics_html = f"""
    <div class="metric-grid">
        <div class="metric-card">
            <div class="metric-label">Props Shown</div>
            <div class="metric-value">{total_props}</div>
            <div class="metric-sub">filtered results</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Avg Hit Rate (L10)</div>
            <div class="metric-value">{avg_hit:.0%}</div>
            <div class="metric-sub">trending performance</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Avg Odds</div>
            <div class="metric-value">{avg_odds:+.0f}</div>
            <div class="metric-sub">mean book line</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Matchup Difficulty</div>
            <div class="metric-value">{avg_matchup:.1f}/10</div>
            <div class="metric-sub">lower = easier</div>
        </div>
    </div>
    """
    st.markdown(metrics_html, unsafe_allow_html=True)

    if filtered_df.empty:
        st.info("No props match your filters.")
        st.stop()

    # -------------------------------------------
    # Mode toggle
    # -------------------------------------------
    view_mode = st.radio(
        "View Mode",
        ["Card grid", "Advanced Table"],
        horizontal=True,
        index=0
    )

    # ======================================================
    # CARD GRID VIEW  (sparkline, L10 fix, opp rank fix)
    # ======================================================
    if view_mode == "Card grid":

        import pandas as pd

        import numpy as np

        def get_spark_values(row):
            """
            Pick the best series for this prop, based on the detected stat.
            Priority: last7_list, then last10_list, then last5_list.
            Returns a plain Python list of numbers, or [].
            """
            stat = detect_stat(row.get("market", ""))  # pts, reb, ast, pra, stl, blk

            if not stat:
                return []

            candidates = [
                f"{stat}_last7_list",
                f"{stat}_last10_list",
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


        def build_sparkline(values, width=80, height=24, color="#0ea5e9"):
            """
            Return a tiny inline SVG sparkline. Uses only inline styles so it
            works inside st.markdown AND st.html.
            """
            # Defensive: only accept numeric sequences
            if not isinstance(values, (list, tuple)):
                return ""

            values = [v for v in values if isinstance(v, (int, float))]

            if len(values) == 0:
                return ""

            # If there's only one point, duplicate so we still see a line
            if len(values) == 1:
                values = values + values

            min_v = min(values)
            max_v = max(values)
            span = max_v - min_v if max_v != min_v else 1.0

            points = []
            for i, v in enumerate(values):
                x = (i / (len(values) - 1)) * width
                y = height - ((v - min_v) / span) * height
                points.append(f"{x:.1f},{y:.1f}")

            svg_points = " ".join(points)

            return f"""
            <svg width="{width}" height="{height}" style="overflow:visible;">
                <polyline
                    points="{svg_points}"
                    fill="none"
                    stroke="{color}"
                    stroke-width="2.2"
                    stroke-linecap="round"
                />
            </svg>
            """


        # == Bookmaker Normalization ==
        def normalize_bookmaker(raw: str) -> str:
            if not raw:
                return ""
            r = raw.strip().lower()
            mapping = {
                "draft": "DraftKings",
                "fanduel": "FanDuel",
                "fd": "FanDuel",
                "mgm": "BetMGM",
                "caes": "Caesars",
                "espn": "ESPN BET",
                "bovada": "Bovada",
                "betrivers": "BetRivers",
                "hard rock": "Hard Rock",
                "pointsbet": "PointsBet",
                "fanatics": "Fanatics",
                "betonline": "BetOnline.ag",
            }
            for k, v in mapping.items():
                if k in r:
                    return v
            return raw.strip()

        # == Filters ==
        MIN_ODDS_FOR_CARD = manual_odds_min
        MAX_ODDS_FOR_CARD = manual_odds_max
        MIN_L10 = manual_l10_min / 100
        REQUIRE_EV_PLUS = True

        def is_ev_plus(row):
            odds = row["price"]
            implied = (100 / (odds + 100)) if odds > 0 else abs(odds) / (abs(odds) + 100)
            return row["hit_rate_last10"] > implied

        def card_good(row):
            if pd.isna(row.get("price")) or pd.isna(row.get("hit_rate_last10")):
                return False
            if not (MIN_ODDS_FOR_CARD <= row["price"] <= MAX_ODDS_FOR_CARD):
                return False
            if row["hit_rate_last10"] < MIN_L10:
                return False
            if REQUIRE_EV_PLUS and not is_ev_plus(row):
                return False
            return True

        # == WOWY merge ==
        card_df = attach_wowy_deltas(filtered_df, wowy_df)

        wowy_cols = ["breakdown", "pts_delta", "reb_delta", "ast_delta",
                     "pra_delta", "pts_reb_delta"]

        def extract_wowy_list(g):
            w = g[g["breakdown"].notna()][wowy_cols]
            return w.to_dict("records")

        w_map = {}
        for (player, team), g in card_df.groupby(["player", "player_team"]):
            w_map[(player, team)] = extract_wowy_list(g)

        card_df["_wowy_list"] = card_df.apply(
            lambda r: w_map.get((r["player"], r["player_team"]), []), axis=1
        )

        def get_l10_avg(row):
            stat = detect_stat(row.get("market", ""))
            col = {
                "pts": "pts_last10",
                "reb": "reb_last10",
                "ast": "ast_last10",
                "pra": "pra_last10",
                "stl": "stl_last10",
                "blk": "blk_last10",
            }.get(stat)
            value = row.get(col)
            return float(value) if pd.notna(value) else None


        # == Opponent Rank ==
        def get_opponent_rank(row):
            stat = detect_stat(row.get("market", ""))
            col = {
                "pts": "opp_pos_pts_rank",
                "reb": "opp_pos_reb_rank",
                "ast": "opp_pos_ast_rank",
                "pra": "opp_pos_pra_rank",
                "stl": "opp_pos_stl_rank",
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

        # == Tags ==
        def build_prop_tags(row):
            tags = []
            odds = row.get("price", 0)
            implied = (100 / (odds + 100)) if odds > 0 else abs(odds) / (abs(odds) + 100)

            if row.get("hit_rate_last10", 0) > implied:
                tags.append(("📈 EV+", "#22c55e"))

            r = get_opponent_rank(row)
            if isinstance(r, int):
                if r <= 10:
                    tags.append(("🔴 Hard", "#ef4444"))
                elif r <= 20:
                    tags.append(("🟡 Neutral", "#eab308"))
                else:
                    tags.append(("🟢 Easy", "#22c55e"))
            return tags

        # Filter
        card_df = card_df[card_df.apply(card_good, axis=1)]
        ranked = card_df.sort_values("hit_rate_last10", ascending=False).reset_index(drop=True)

        # Pagination
        page_size = 30
        total_cards = len(ranked)
        total_pages = max(1, (total_cards + page_size - 1) // page_size)

        st.write(f"Showing {total_cards} props • {total_pages} pages")

        page = st.number_input(
            "Page", min_value=1, max_value=total_pages, value=1, step=1, key="card_page_number"
        )

        start = (page - 1) * page_size
        end = start + page_size
        page_df = ranked.iloc[start:end]

        st.markdown("""
            <div style="max-height:1100px; overflow-y:auto; padding-right:12px;">
        """, unsafe_allow_html=True)

        cols = st.columns(4)
        has_html = hasattr(st, "html")

        # ==============================
        # CARD LOOP
        # ==============================
        for idx, row in page_df.iterrows():

            col = cols[idx % 4]
            with col:

                player = row.get("player", "")
                pretty_market = MARKET_DISPLAY_MAP.get(row.get("market", ""), row.get("market", ""))
                bet_type = str(row.get("bet_type", "")).upper()
                line = row.get("line", "")

                odds = int(row.get("price", 0))
                implied_prob = (100 / (odds + 100)) if odds > 0 else abs(odds) / (abs(odds) + 100)
                hit10 = row.get("hit_rate_last10", 0.0)

                # L10 Avg
                l10_avg = get_l10_avg(row)
                l10_avg_display = f"{l10_avg:.1f}" if l10_avg is not None else "-"

                # Opp Rank
                opp_rank = get_opponent_rank(row)
                if isinstance(opp_rank, int):
                    rank_display = opp_rank
                    rank_color = rank_to_color(opp_rank)
                else:
                    rank_display = "-"
                    rank_color = "#9ca3af"

                stat = detect_stat(row.get("market", ""))

                # DEBUG: verify what we’re feeding into the sparkline
                st.write(
                    "DEBUG SPARK:", row["player"], "stat =", stat,
                    "vals =", get_spark_values(row)
                )

                spark_vals = get_spark_values(row)
                spark_html = build_sparkline(spark_vals)


                # Logos
                player_team = normalize_team_code(row.get("player_team", ""))
                opp_team = normalize_team_code(row.get("opponent_team", ""))

                home_logo = TEAM_LOGOS_BASE64.get(player_team, "")
                opp_logo = TEAM_LOGOS_BASE64.get(opp_team, "")

                logos_html = f"""
                    <div style="display:flex;align-items:center;justify-content:flex-end;gap:6px;">
                        <img src="{home_logo}" style="height:18px;border-radius:4px;" />
                        <span style="font-size:0.7rem;color:#9ca3af;">vs</span>
                        <img src="{opp_logo}" style="height:18px;border-radius:4px;" />
                    </div>
                """ if home_logo else ""

                # Bookmaker
                book = normalize_bookmaker(row.get("bookmaker", ""))
                book_logo_b64 = SPORTSBOOK_LOGOS_BASE64.get(book, "")

                if book_logo_b64:
                    book_html = f"""
                        <img src="{book_logo_b64}" 
                             style="height:26px;max-width:90px;object-fit:contain;" />
                    """
                else:
                    book_html = f"""
                        <div style="
                            padding:3px 10px;
                            border-radius:8px;
                            background:rgba(255,255,255,0.08);
                            border:1px solid rgba(255,255,255,0.15);
                            font-size:0.7rem;">
                            {book}
                        </div>
                    """

                # Tags + WOWY
                tags_html = build_tags_html(build_prop_tags(row))
                wowy_html = build_wowy_block(row)

                # Card Layout
                card_html = f"""
                <div class="prop-card">

                    <!-- TOP CENTER -->
                    <div style="text-align:center; margin-bottom:6px;">
                        <div class="prop-player" style="font-size:1.05rem;font-weight:700;">
                            {player}
                        </div>
                        <div class="prop-market" style="font-size:0.82rem;color:#9ca3af;margin-top:2px;">
                            {pretty_market} • {bet_type} {line}
                        </div>
                    </div>

                    <hr style="border:0;border-top:1px solid rgba(255,255,255,0.08);margin:6px 0 10px 0;" />

                    <!-- MIDDLE SPLIT -->
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                        <div style="padding-right:8px;flex:1;">{spark_html}</div>
                        <div style="text-align:right; flex-shrink:0;">
                            {book_html}
                            <div style="margin-top:4px;">{logos_html}</div>
                        </div>
                    </div>

                    <!-- TAGS -->
                    <div style="display:flex;justify-content:center;margin-bottom:6px;">
                        {tags_html}
                    </div>

                    <!-- BOTTOM METRICS -->
                    <div class="prop-meta" style="margin-top:2px;">
                        <div>
                            <div style="color:#e5e7eb;font-size:0.8rem;">{odds:+d}</div>
                            <div style="font-size:0.7rem;">Imp: {implied_prob:.0%}</div>
                        </div>
                        <div>
                            <div style="color:#e5e7eb;font-size:0.8rem;">L10 Hit: {hit10:.0%}</div>
                            <div style="font-size:0.7rem;">L10 Avg: {l10_avg_display}</div>
                        </div>
                        <div>
                            <div style="color:{rank_color};font-size:0.8rem;font-weight:700;">
                                {rank_display}
                            </div>
                            <div style="font-size:0.7rem;">Opp Rank</div>
                        </div>
                    </div>

                    {wowy_html}

                </div>
                """

                st.markdown(card_html, unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)
        st.caption("Card view updated: centered header, sparkline, L10 fixes, opponent-rank difficulty, NA-safe logic.")

    # ======================================================
    # ADVANCED TABLE VIEW
    # ======================================================
    else:

        df = filtered_df.copy()

        df["home_team"] = df["home_team"].astype(str).str.upper()
        df["opponent_team"] = df["opponent_team"].astype(str).str.upper()

        df["Implied Prob"] = np.where(
            df["price"] > 0,
            100 / (df["price"] + 100),
            abs(df["price"]) / (abs(df["price"]) + 100),
        ) * 100

        df["Hit5"] = (df["hit_rate_last5"] * 100).round(0)
        df["Hit10"] = (df["hit_rate_last10"] * 100).round(0)
        df["Hit20"] = (df["hit_rate_last20"] * 100).round(0)

        df["Edge_raw"] = df["Hit10"] - df["Implied Prob"]
        df["Edge"] = df["Edge_raw"].apply(
            lambda x: f"+{x:.1f}%" if x > 0 else f"{x:.1f}%"
        )

        df["Matchup30"] = (
            df["matchup_difficulty_score"].fillna(5).clip(1, 10) * 3
        ).round(0)

        df["line"] = df["line"].astype(float)

        df["Sparkline"] = df.apply(
            lambda r: [
                int(r["Hit5"]), int(r["Hit10"]), int(r["Hit20"]),
                int(np.random.randint(30, 90)), int(np.random.randint(30, 90)),
            ],
            axis=1,
        )

        grid_df = pd.DataFrame({
            "Player": df["player"],
            "Market": df["market"].apply(lambda m: MARKET_DISPLAY_MAP.get(m, m)),
            "Line": df["line"],
            "Label": df["bet_type"],
            "Odds": df["price"],
            "Book": df["bookmaker"],
            "Hit5": df["Hit5"],
            "Hit10": df["Hit10"],
            "Hit20": df["Hit20"],
            "Spark": df["Sparkline"],
            "ImpProb": df["Implied Prob"],
            "Edge_raw": df["Edge_raw"],
            "Edge": df["Edge"],
            "Matchup30": df["Matchup30"],
        })

        # --- AG Grid render config (unchanged from your version) ---
        # (leaving your renderer code untouched)
        # ------------------------------------------------------------
        sparkline_renderer = JsCode("""
            function(params){
                const v = params.value;
                if (!v || !Array.isArray(v)) return '';
                const maxVal = Math.max(...v);
                const minVal = Math.min(...v);
                const height = 22;
                const width = 60;
                function scaleY(val){
                    return height - ((val - minVal) / (maxVal - minVal + 0.0001)) * height;
                }
                let pts = v.map((val,i)=>{
                    const x = (i / (v.length - 1)) * width;
                    return `${x},${scaleY(val)}`;
                }).join(" ");
                return `
                    <svg width="${width}" height="${height}">
                        <polyline points="${pts}" class="sparkline" />
                    </svg>
                `;
            }
        """)

        odds_formatter = JsCode("""
            function(params){
                if (params.value == null) return '';
                return params.value > 0 ? '+' + params.value : params.value.toString();
            }
        """)

        percent_formatter = JsCode("""
            function(params){
                if (params.value == null) return '';
                return params.value.toFixed(0) + '%';
            }
        """)

        matchup_formatter = JsCode("""
            function(params){
                if (params.value == null) return '';
                return params.value.toFixed(0) + '/30';
            }
        """)

        row_style_js = JsCode("""
            function(params){
                const e = params.data.Edge_raw;
                if (e >= 8) return { backgroundColor: "rgba(34,197,94,0.08)" };
                if (e >= 3) return { backgroundColor: "rgba(59,130,246,0.08)" };
                if (e <= -5) return { backgroundColor: "rgba(239,68,68,0.08)" };
                return {};
            }
        """)

        hit_style = JsCode("""
            function(params){
                const p = params.value;
                const hue = 120 * (p / 100);
                return {
                    backgroundColor: `hsl(${hue},85%,40%)`,
                    color: 'white',
                    fontWeight: 700,
                    textAlign: 'center'
                };
            }
        """)

        edge_style = JsCode("""
            function(params){
                const e = params.data.Edge_raw;
                const t = (e + 30) / 60.0;
                const clipped = Math.max(0, Math.min(1, t));
                const hue = 120 * clipped;
                return {
                    backgroundColor: `hsl(${hue},80%,35%)`,
                    color: 'white',
                    fontWeight: 700,
                    textAlign: 'center'
                };
            }
        """)

        matchup_style = JsCode("""
            function(params){
                const v = params.value;
                const t = (v - 1) / 29.0;
                const clipped = Math.max(0, Math.min(1, t));
                const hue = 120 * clipped;
                return {
                    backgroundColor: `hsl(${hue},70%,35%)`,
                    color: 'white',
                    fontWeight: 700,
                    textAlign: 'center'
                };
            }
        """)

        gb = GridOptionsBuilder.from_dataframe(grid_df)
        gb.configure_default_column(
            sortable=True,
            resizable=True,
            minWidth=120,
            width=130,
            maxWidth=200,
            cellStyle={"textAlign": "center"},
        )
        gb.configure_column("*", filter=True)
        gb.configure_selection("multiple", use_checkbox=True)

        gb.configure_grid_options(
            getRowStyle=row_style_js,
            suppressSizeToFit=True,
            suppressAutoSize=True,
            suppressHorizontalScroll=False,
            domLayout="normal",
        )

        gb.configure_column("Player", pinned="left", minWidth=140)
        gb.configure_column("Odds", valueFormatter=odds_formatter, width=95)

        gb.configure_column("Hit5", header_name="L5", valueFormatter=percent_formatter, cellStyle=hit_style, width=75)
        gb.configure_column("Hit10", header_name="L10", valueFormatter=percent_formatter, cellStyle=hit_style, width=75)
        gb.configure_column("Hit20", header_name="L20", valueFormatter=percent_formatter, cellStyle=hit_style, width=75)

        gb.configure_column("Spark", header_name="Trend", cellRenderer=sparkline_renderer, width=100, filter=False)
        gb.configure_column("ImpProb", header_name="Imp%", valueFormatter=percent_formatter, width=80)

        gb.configure_column("Edge_raw", hide=True)
        gb.configure_column("Edge", cellStyle=edge_style, width=100)
        gb.configure_column("Matchup30", header_name="Matchup", valueFormatter=matchup_formatter, cellStyle=matchup_style, width=100)

        grid_response = AgGrid(
            grid_df,
            gridOptions=gb.build(),
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            fit_columns_on_grid_load=False,
            theme="balham",
            allow_unsafe_jscode=True,
            height=550,
        )

        selected = grid_response.get("selected_rows", [])

        # Save selected bets
        if selected:
            sel_df = pd.DataFrame(selected)[
                ["Player", "Market", "Line", "Label", "Odds", "Book"]
            ].rename(columns={
                "Player": "player",
                "Market": "market",
                "Line": "line",
                "Label": "bet_type",
                "Odds": "price",
                "Book": "bookmaker",
            })

            st.session_state.saved_bets = sel_df.drop_duplicates().to_dict("records")
            replace_saved_bets_in_db(user_id, st.session_state.saved_bets)

            st.success(f"{len(sel_df)} bet(s) saved.")
        else:
            st.session_state.saved_bets = []
            replace_saved_bets_in_db(user_id, [])

# ------------------------------------------------------
# TAB 2 — TREND LAB (Dev)
# ------------------------------------------------------
with tab2:
    st.subheader("Trend Lab (Real History + Dynamic Line)")

    c1, c2, c3 = st.columns([1.2, 1.2, 1])
    with c1:
        player = st.selectbox(
            "Player", sorted(props_df["player"].dropna().unique())
        )
    with c2:
        stat_label = st.selectbox("Stat", ["Points", "Rebounds", "Assists", "P+R+A", "Steals", "Blocks", "3PT Made"])
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

    df = history_df[history_df["player_clean"] == player_clean].copy()

    df[stat] = pd.to_numeric(df[stat], errors="coerce")
    df = df[df[stat].notna()]

    df = (
        df.sort_values("game_date")
        .drop_duplicates(subset=["game_date"], keep="last")
        .reset_index(drop=True)
    )

    df = df.sort_values("game_date").tail(n_games).reset_index(drop=True)

    if df.empty:
        st.info("No historical data found for this selection.")
    else:
        df["date_str"] = df["game_date"].dt.strftime("%b %d")

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

        df["hit"] = df[stat] > float(line)
        df["rolling"] = df[stat].rolling(window=5, min_periods=1).mean()

        hit_rate = df["hit"].mean()
        avg_last5 = df[stat].tail(5).mean()
        std_dev = df[stat].std()
        last_game_value = df[stat].iloc[-1]

        metric_row = f"""
        <div class="metric-grid" style="margin-top:0.25rem;margin-bottom:1rem;">
            <div class="metric-card">
                <div class="metric-label">Hit Rate</div>
                <div class="metric-value">{hit_rate:.0%}</div>
                <div class="metric-sub">{df['hit'].sum()} of {len(df)} games</div>
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
            for _, row in df.iterrows()
        ]

        fig = go.Figure()

        fig.add_bar(
            x=df["date_str"],
            y=df[stat],
            marker_color=["#22c55e" if h else "#ef4444" for h in df["hit"]],
            hovertext=hover,
            hoverinfo="text",
            name="Game Result",
        )

        fig.add_trace(
            go.Scatter(
                x=df["date_str"],
                y=df["rolling"],
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
        for h in df["hit"]:
            ribbon_html += (
                "<div style='width:14px;height:14px;border-radius:3px;background:#22c55e;'></div>"
                if h
                else "<div style='width:14px;height:14px;border-radius:3px;background:#ef4444;'></div>"
            )
        ribbon_html += "</div>"
        st.markdown(ribbon_html, unsafe_allow_html=True)

        table_df = df.copy()
        table_df["Outcome"] = table_df["hit"].map({True: "Hit", False: "Miss"})
        table_df_display = table_df[["date_str", "opponent_team", stat, "Outcome"]]
        table_df_display.columns = ["Date", "Opponent", stat_label, "Outcome"]

        st.dataframe(table_df_display, use_container_width=True, hide_index=True)

# ------------------------------------------------------
# TAB 3 — SAVED BETS (DB-backed)
# ------------------------------------------------------
with tab3:
    st.subheader("Saved Bets")

    if not st.session_state.saved_bets:
        st.info(
            "No saved bets yet — select rows in the Props Overview table (Advanced Table view)."
        )
    else:
        df_saved = pd.DataFrame(st.session_state.saved_bets)
        df_saved_display = df_saved.rename(
            columns={
                "player": "Player",
                "market": "Market",
                "line": "Line",
                "bet_type": "Label",
                "price": "Price",
                "bookmaker": "Book",
            }
        )

        st.dataframe(df_saved_display, use_container_width=True, hide_index=True)

        csv = df_saved.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Saved Bets (CSV)",
            data=csv,
            file_name="saved_bets.csv",
            mime="text/csv",
        )

#-------------------------------------------------
# TAB 4 Depth Chart & Injury Report
#-------------------------------------------------
with tab4:
    st.subheader("")

    # --------------------------------------------------------
    # GLOBAL CSS — SPACIOUS CARD GRID + INJURY BADGE SUPPORT
    # --------------------------------------------------------
    st.html("""
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
""")

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

    # Filter
    team_depth = depth_df[depth_df["team_number"] == selected_team_number].copy()
    team_injuries = injury_df[injury_df["team_number"] == selected_team_number].copy()

    # Prepare fast lookup for injuries
    injury_lookup = (
        team_injuries.groupby("player_id")
        .tail(1)
        .set_index("player_id")[["status", "description"]]
        .to_dict(orient="index")
    )

    # ----------------------------
    # TEAM HEADER (more spacious)
    # ----------------------------
    logo = TEAM_LOGOS_BASE64.get(selected_abbr, "")
    st.html(
        f"<div class='header-flex'>"
        f"<img src='{logo}' style='height:55px;border-radius:12px;'/>"
        f"<div>"
        f"<div style='font-size:1.55rem;font-weight:700;color:#e5e7eb;'>{selected_name}</div>"
        f"<div style='font-size:0.9rem;color:#9ca3af;'>Depth chart & injury status</div>"
        f"</div></div>"
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

        # Wider column spacing: max 3 per row
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

                    # Injury badge logic
                    injury_html = ""
                    if player_id in injury_lookup:
                        st_val = injury_lookup[player_id]["status"]
                        st_low = st_val.lower()
                        if "out" in st_low:
                            badge_color = "background:#ef4444;"
                        elif "question" in st_low or "doubt" in st_low:
                            badge_color = "background:#eab308;"
                        else:
                            badge_color = "background:#3b82f6;"

                        injury_html = (
                            f"<span class='injury-badge' style='{badge_color}'>{st_val.upper()}</span>"
                        )

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

                    st.html(html)

    # ------------------------------------------------------
    # INJURY REPORT (RIGHT)
    # ------------------------------------------------------
    with col_right:
        st.markdown("## 🏥 Injury Report")

        if team_injuries.empty:
            st.success("No reported injuries.")
        else:
            last_ts = team_injuries["snapshot_ts"].max()
            st.caption(f"Last update: {last_ts.strftime('%b %d, %Y %I:%M %p')}")

            for _, r in (
                team_injuries.sort_values("snapshot_ts")
                .groupby("player_id")
                .tail(1)
                .sort_values("status")
                .iterrows()
            ):

                name = f"{r['first_name']} {r['last_name']}"
                status = r["status"]
                ret = r["return_date_raw"]
                desc = r["description"]

                st_low = status.lower()
                if "out" in st_low:
                    status_color = "background:#ef4444;"
                elif "question" in st_low or "doubt" in st_low:
                    status_color = "background:#eab308;"
                else:
                    status_color = "background:#3b82f6;"

                html = (
                    f"<div class='injury-card'>"
                    f"  <div style='display:flex;justify-content:space-between;'>"
                    f"    <div style='font-size:1.05rem;font-weight:600;color:white;'>{name}</div>"
                    f"    <div class='injury-badge' style='{status_color}'>{status.upper()}</div>"
                    f"  </div>"
                    f"  <div style='font-size:0.85rem;color:#e5e7eb;margin-top:6px;'><b>Return:</b> {ret}</div>"
                    f"  <div style='font-size:0.85rem;color:#e5e7eb;margin-top:6px;'>{desc}</div>"
                    f"</div>"
                )

                st.html(html)

# ------------------------------------------------------
# TAB 5 — WOWY ANALYZER
# ------------------------------------------------------
with tab5:
    st.subheader("🔀 WOWY (With/Without You) Analyzer")

    st.markdown("""
    Below is the full WOWY table — showing how each player's production
    changes when a specific teammate is **OUT**.
    
    Sort any column to explore the biggest deltas.
    """)

    # Prepare WOWY table
    wow = wowy_df.copy()

    # Sort by biggest points impact
    wow = wow.sort_values("pts_delta", ascending=False)

    # Build display table
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
# LAST UPDATED
# ------------------------------------------------------
now = datetime.now(EST)
st.sidebar.markdown(f"**Last Updated:** {now.strftime('%b %d, %I:%M %p')} ET")