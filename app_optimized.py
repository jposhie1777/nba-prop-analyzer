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
  pra,
  stl,
  blk
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
                    Explore props, trends, saved bets, and parlay scenarios using live BigQuery data.
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

SPORTSBOOK_LOGOS = {
    "DraftKings": "https://storage.googleapis.com/bvb-public/sportsbooks/draftkings.png",
    "FanDuel": "https://storage.googleapis.com/bvb-public/sportsbooks/fanduel.png",
    "BetMGM": "https://storage.googleapis.com/bvb-public/sportsbooks/betmgm.png",
    "Caesars": "https://storage.googleapis.com/bvb-public/sportsbooks/caesars.png",
    "ESPN BET": "https://storage.googleapis.com/bvb-public/sportsbooks/espnbet.png",
    "BetOnline.ag": "https://storage.googleapis.com/bvb-public/sportsbooks/betonline.png",
    "Bovada": "https://storage.googleapis.com/bvb-public/sportsbooks/bovada.png",
    "BetRivers": "https://storage.googleapis.com/bvb-public/sportsbooks/betrivers.png",
    "PointsBet": "https://storage.googleapis.com/bvb-public/sportsbooks/pointsbet.png",
    "Hard Rock": "https://storage.googleapis.com/bvb-public/sportsbooks/hardrock.png",
    "Fanatics": "https://storage.googleapis.com/bvb-public/sportsbooks/fanatics.png",
}



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


def build_prop_tags(row):
    tags = []
    if row.get("hit_rate_last10", 0) >= 0.70:
        tags.append(("🔥 HOT", "#f97316"))

    odds = row.get("price", 0)
    if odds > 0:
        implied = 100 / (odds + 100)
    else:
        implied = abs(odds) / (abs(odds) + 100) if odds != 0 else 0

    if row.get("hit_rate_last10", 0) > implied:
        tags.append(("📈 EV+", "#22c55e"))

    matchup = float(row.get("matchup_difficulty_score", 50))
    if matchup <= 33:
        tags.append(("🔴 Hard", "#ef4444"))
    elif matchup >= 67:
        tags.append(("🟢 Easy", "#22c55e"))
    else:
        tags.append(("🟡 Neutral", "#eab308"))

    return tags

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

@st.cache_data(show_spinner=False)
def logo_to_base64(url: str) -> str:
    if not url:
        return ""
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        b64 = base64.b64encode(resp.content).decode("utf-8")
        return f"data:image/png;base64,{b64}"
    except Exception:
        return ""

@st.cache_data(show_spinner=False)
def build_team_logo_b64_map(team_logos: dict) -> dict:
    out = {}
    for code, url in team_logos.items():
        out[code] = logo_to_base64(url)
    return out

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

TEAM_LOGOS_BASE64 = build_team_logo_b64_map(TEAM_LOGOS)
SPORTSBOOK_LOGOS_BASE64 = build_team_logo_b64_map(SPORTSBOOK_LOGOS)


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


@st.cache_data(show_spinner=True)
def load_history():
    df = bq_client.query(HISTORICAL_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["opponent_team"] = df["opponent_team"].fillna("").astype(str)
    return df


props_df = load_props()
history_df = load_history()

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

    # Global Min L10 Hit Rate (applies to ALL props, including steals/blocks)
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
# TABS (Dev 4 tabs + Prop Analytics)
# ------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "🧮 Props Overview",
        "📈 Trend Lab",
        "📋 Saved Bets",
        "🎟️ Bet Slip Playground",
        "📊 Prop Analytics",
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
    # CARD GRID VIEW (Option B — Steals/Blocks bypass L10)
    # ======================================================
    if view_mode == "Card grid":

        # -------------------------
        # Helpers
        # -------------------------
        def normalize_bookmaker(name: str) -> str:
            if not name:
                return ""
            n = name.strip().lower()
            if "draft" in n:
                return "DraftKings"
            if "fanduel" in n or n == "fd":
                return "FanDuel"
            if "mgm" in n:
                return "BetMGM"
            if "caes" in n:
                return "Caesars"
            if "espn" in n:
                return "ESPN BET"
            return name

        MIN_ODDS_FOR_CARD = manual_odds_min
        MAX_ODDS_FOR_CARD = manual_odds_max
        MIN_L10 = manual_l10_min / 100
        REQUIRE_EV_PLUS = True

        def is_ev_plus(row):
            odds = row["price"]
            implied = (
                100 / (odds + 100)
                if odds > 0
                else abs(odds) / (abs(odds) + 100)
            )
            return row["hit_rate_last10"] > implied

        def card_good(row):
            """
            Card Grid Option B:
            - Must meet odds range
            - Must be EV+
            - Steals/Blocks bypass L10-but others must meet manual L10 threshold
            """
            if pd.isna(row.get("price")) or pd.isna(row.get("hit_rate_last10")):
                return False

            if not (MIN_ODDS_FOR_CARD <= row["price"] <= MAX_ODDS_FOR_CARD):
                return False

            stat = detect_stat(row.get("market", ""))

            if stat not in ("stl", "blk") and row["hit_rate_last10"] < MIN_L10:
                return False

            if REQUIRE_EV_PLUS and not is_ev_plus(row):
                return False

            return True

        # Apply card-grid filter
        card_df = filtered_df[filtered_df.apply(card_good, axis=1)]

        ranked = (
            card_df.sort_values("hit_rate_last10", ascending=False)
            if not card_df.empty
            else card_df
        ).reset_index(drop=True)

        # Pagination
        page_size = 30
        total_cards = len(ranked)
        total_pages = max(1, (total_cards + page_size - 1) // page_size)

        st.write(f"Showing {total_cards} props • {total_pages} pages")

        page = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key="card_page_number"
        )

        start = (page - 1) * page_size
        end = start + page_size
        page_df = ranked.iloc[start:end]

        st.markdown("""
            <div style="
                max-height: 1100px;
                overflow-y: auto;
                padding-right: 12px;
            ">
        """, unsafe_allow_html=True)

        cols = st.columns(4)
        has_html = hasattr(st, "html")

        for idx, row in page_df.iterrows():
            col = cols[idx % 4]
            with col:

                player = row.get("player", "")
                pretty_market = MARKET_DISPLAY_MAP.get(
                    row.get("market", ""), row.get("market", "")
                )
                bet_type = str(row.get("bet_type", "")).upper()
                line = row.get("line", "")

                odds = int(row.get("price", 0))
                hit10 = row.get("hit_rate_last10", 0.0)
                hit20 = row.get("hit_rate_last20", 0.0)
                matchup = row.get("matchup_difficulty_score", 50)

                implied_prob = (
                    100 / (odds + 100)
                    if odds > 0
                    else abs(odds) / (abs(odds) + 100)
                )

                player_team = normalize_team_code(row.get("player_team", ""))
                opp_team = normalize_team_code(row.get("opponent_team", ""))

                home_logo = TEAM_LOGOS_BASE64.get(player_team, "")
                opp_logo = TEAM_LOGOS_BASE64.get(opp_team, "")

                if home_logo and opp_logo:
                    logos_html = f"""
                        <div style="display:flex;align-items:center;justify-content:flex-end;gap:6px;">
                            <img src="{home_logo}" style="height:18px;border-radius:4px;" />
                            <span style="font-size:0.7rem;color:#9ca3af;">vs</span>
                            <img src="{opp_logo}" style="height:18px;border-radius:4px;" />
                        </div>
                    """
                else:
                    logos_html = f"""
                        <div style='font-size:0.75rem;color:#9ca3af;'>
                            {row.get("home_team","")} vs {row.get("opponent_team","")}
                        </div>
                    """

                book = normalize_bookmaker(row.get("bookmaker", ""))
                book_logo_b64 = SPORTSBOOK_LOGOS_BASE64.get(book, "")

                if book_logo_b64:
                    book_html = f'<img src="{book_logo_b64}" style="height:24px;border-radius:4px;" />'
                else:
                    book_html = f'<div class="pill-book">{book}</div>'

                tags_html = build_tags_html(build_prop_tags(row))

                card_html = f"""
                <div class="prop-card">
                    <div class="prop-headline">
                        <div>
                            <div class="prop-player">{player}</div>
                            <div class="prop-market">
                                {pretty_market} • {bet_type} {line}
                            </div>
                            <div style="margin-top:4px;">{tags_html}</div>
                        </div>
                        <div style="text-align:right;">
                            {book_html}
                            {logos_html}
                        </div>
                    </div>

                    <div class="prop-meta">
                        <div>
                            <div style="color:#e5e7eb;font-size:0.8rem;">{odds:+d}</div>
                            <div style="font-size:0.7rem;">Imp: {implied_prob:.0%}</div>
                        </div>
                        <div>
                            <div style="color:#e5e7eb;font-size:0.8rem;">L10: {hit10:.0%}</div>
                            <div style="font-size:0.7rem;">L20: {hit20:.0%}</div>
                        </div>
                        <div>
                            <div style="color:#e5e7eb;font-size:0.8rem;">{matchup:.0f}/100</div>
                            <div style="font-size:0.7rem;">Difficulty</div>
                        </div>
                    </div>
                </div>
                """

                if has_html:
                    st.html(card_html)
                else:
                    st.markdown(card_html, unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)

        st.caption("Card view is visual-only — use the table to save legs.")

    # ======================================================
    # ADVANCED TABLE VIEW  (FULL RESTORE)
    # ======================================================
    else:

        df = filtered_df.copy()

        # Normalize team codes
        df["home_team"] = df["home_team"].astype(str).str.upper()
        df["opponent_team"] = df["opponent_team"].astype(str).str.upper()

        # Add derived fields
        df["Implied Prob"] = np.where(
            df["price"] > 0,
            100 / (df["price"] + 100),
            np.abs(df["price"]) / (np.abs(df["price"]) + 100),
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

        # Fake trend sparkline (kept from previous version)
        df["Sparkline"] = df.apply(
            lambda r: [
                int(r["Hit5"]),
                int(r["Hit10"]),
                int(r["Hit20"]),
                int(np.random.randint(30, 90)),
                int(np.random.randint(30, 90)),
            ],
            axis=1,
        )

        # Build grid dataframe
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

        # -----------------------------
        # Render AG-Grid (unchanged)
        # -----------------------------
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
                const hue = 120 * Math.max(0, Math.min(1, t));
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
                const hue = 120 * t;
                return {
                    backgroundColor: `hsl(${hue},70%,35%)`,
                    color: 'white',
                    fontWeight: 700,
                    textAlign: 'center'
                };
            }
        """)

        # Build the grid options
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

# ------------------------------------------------------
# TAB 4 — BET SLIP PLAYGROUND (Dev)
# ------------------------------------------------------
with tab4:
    st.subheader("Bet Slip Playground (Real Calculations)")

    if not st.session_state.saved_bets:
        st.info("Save some bets from Props Overview first (Advanced Table).")
    else:
        slip_df = pd.DataFrame(st.session_state.saved_bets).copy()
        slip_df["Add to Slip"] = True
        slip_df["Stake"] = 0.0

        def american_to_decimal(odds: float) -> float:
            odds = float(odds)
            if odds > 0:
                return 1 + (odds / 100)
            return 1 + (100 / abs(odds))

        slip_df["decimal_odds"] = slip_df["price"].astype(float).apply(
            american_to_decimal
        )

        st.markdown("#### 💾 Legs in Slip (DB-backed, real data)")

        edited_slip = st.data_editor(
            slip_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Add to Slip": st.column_config.CheckboxColumn("Add"),
                "Stake": st.column_config.NumberColumn("Stake ($)", min_value=0.0),
                "decimal_odds": st.column_config.NumberColumn(
                    "Decimal", format="%.3f"
                ),
            },
            key="bet_slip_editor_real",
        )

        mask = edited_slip["Add to Slip"].fillna(False).astype(bool)
        selected = edited_slip.loc[mask]

        if selected.empty:
            st.info("Use the **Add** checkbox to include legs in your slip.")
        else:
            c1, c2 = st.columns([2, 1])

            with c1:
                st.markdown("##### Active Legs")
                st.dataframe(
                    selected[
                        [
                            "player",
                            "market",
                            "line",
                            "bet_type",
                            "price",
                            "Stake",
                            "decimal_odds",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

            def calc_payout(odds, stake):
                odds = float(odds)
                stake = float(stake)
                if stake <= 0:
                    return 0.0
                if odds > 0:
                    return stake * (odds / 100)
                return stake * (100 / abs(odds))

            selected["Payout"] = selected.apply(
                lambda r: calc_payout(r["price"], r["Stake"]), axis=1
            )

            total_stake = selected["Stake"].sum()
            total_return = (selected["Stake"] + selected["Payout"]).sum()

            with c2:
                st.markdown("##### Singles Summary")
                st.metric("Total Stake", f"${total_stake:.2f}")
                st.metric("Total Return (Singles)", f"${total_return:.2f}")

            st.markdown("---")
            st.markdown("#### 🎯 Parlay Simulator")

            col1, col2, col3 = st.columns([1.4, 1.4, 2])

            with col1:
                parlay_stake = st.number_input(
                    "Parlay Stake ($)", min_value=0.0, value=10.0, step=1.0
                )

            with col2:
                combined_decimal = selected["decimal_odds"].prod()
                st.metric("Combined Decimal", f"{combined_decimal:.3f}")

            with col3:
                parlay_payout = (
                    parlay_stake * (combined_decimal - 1) if parlay_stake > 0 else 0
                )
                st.metric("Parlay Payout", f"${parlay_payout:.2f}")

# ------------------------------------------------------
# TAB 5 — PROP ANALYTICS (Original Production Tab)
# ------------------------------------------------------
with tab5:
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
