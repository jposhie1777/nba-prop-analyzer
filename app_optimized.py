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
import re
from functools import lru_cache
from rapidfuzz import fuzz, process
from textwrap import dedent


DEBUG_LANDING = True   #  <-- ADD THIS

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
    initial_sidebar_state="collapsed",
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
  ra_last10_list,
  last5_dates,
  last7_dates,
  last10_dates,
  last20_dates

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

GAME_ODDS_SQL = """
SELECT
  `Home Team` AS home_team,
  `Away Team` AS away_team,
  Bookmaker     AS bookmaker,
  Market        AS market,
  Outcome       AS outcome,
  Line          AS line,
  Price         AS price,
  `Start Time`  AS start_time,
  Game          AS game
FROM `graphite-flare-477419-h7.nba.nba_game_odds`
WHERE DATE(`Start Time`) = CURRENT_DATE()
"""

# ------------------------------------------------------
# NCAA GAME ANALYTICS SQL
# ------------------------------------------------------
NCAAB_GAME_ANALYTICS_SQL = f"""
SELECT *
FROM `{PROJECT_ID}.ncaa_data.ncaab_game_analytics`
ORDER BY start_time
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

def render_landing_nba_games():
    st.subheader("🏀 NBA Games Today")

    try:
        sql = """
        SELECT
            game_id,
            game_date,
            start_time_est,
            start_time_formatted,
            home_team,
            visitor_team,
            home_team_id,
            visitor_team_id,
            is_live,
            is_upcoming
        FROM `nba_prop_analyzer.game_report`
        WHERE game_date = CURRENT_DATE("America/New_York")
        ORDER BY start_time_est
        """

        df = bq_client.query(sql).to_dataframe()

        if df.empty:
            st.info("No NBA games scheduled for today.")
            return

        # -------------------------------
        # Render game list cleanly
        # -------------------------------
        for _, g in df.iterrows():

            # Team logos with safe fallback
            try:
                away_logo = f"https://a.espncdn.com/i/teamlogos/nba/500/{int(g['visitor_team_id'])}.png"
                home_logo = f"https://a.espncdn.com/i/teamlogos/nba/500/{int(g['home_team_id'])}.png"
            except Exception:
                fallback = "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"
                away_logo = home_logo = fallback

            # Status text
            if g.get("is_live"):
                status = "<span style='color:#ff4d4d; font-weight:600;'>LIVE</span>"
            elif g.get("is_upcoming"):
                status = "<span style='color:#4dabf5; font-weight:600;'>Upcoming</span>"
            else:
                status = "<span style='color:#9aa4b2;'>Final</span>"

            # Render card
            st.markdown(
                f"""
                <div style="display:flex; align-items:center; gap:14px; margin-bottom:6px;">
                    <img src="{away_logo}" width="44" style="border-radius:6px;" />
                    <span style="font-weight:600;">vs</span>
                    <img src="{home_logo}" width="44" style="border-radius:6px;" />
                </div>

                <div style="color:#9aa4b2; font-size:14px; margin-bottom:4px;">
                    {g['start_time_formatted']} ET • {status}
                </div>

                <div style="height:12px"></div>
                """,
                unsafe_allow_html=True
            )

    except Exception:
        st.info("NBA games for today will appear here.")

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
    return jwt.decode(id_token, options={"verify_signature": False, "verify_aud": False})

# ------------------------------------------------------
# AUTH FLOW – Handles Auth0 callback ONLY
# ------------------------------------------------------
def ensure_logged_in():
    if "user" in st.session_state and "user_id" in st.session_state:
        return

    try:
        qp = st.query_params
    except AttributeError:
        qp = st.experimental_get_query_params()

    code = qp.get("code")
    if isinstance(code, list):
        code = code[0]

    if not code:
        return  # no Auth0 callback yet

    try:
        token_data = exchange_code_for_token(code)
        id_token = token_data.get("id_token")
        claims = decode_id_token(id_token)

        auth0_sub = claims.get("sub")
        email = claims.get("email", "")

        user_row = get_or_create_user(auth0_sub, email)
        st.session_state["user"] = {
            "auth0_sub": auth0_sub,
            "email": email,
        }
        st.session_state["user_id"] = user_row["id"]

        # Clear ?code from URL
        try:
            st.experimental_set_query_params()
        except:
            pass

        st.rerun()

    except Exception as e:
        st.error(f"❌ Login failed: {e}")
        st.stop()

# ------------------------------------------------------
# AUTH FLOW – HANDLE AUTH0 CALLBACK FIRST
# ------------------------------------------------------
ensure_logged_in()

# ------------------------------------------------------
# NOT LOGGED IN → SHOW LANDING SCREEN (ONLY PLACE)
# ------------------------------------------------------
if "user" not in st.session_state:
    st.title("Pulse Sports Analytics")
    st.caption("Daily games, props, trends, and analytics")

    # 🔐 LOGIN CTA — TOP & PROMINENT
    login_url = get_auth0_authorize_url()
    st.markdown(
        f"""
        <div style="
            margin: 14px 0 22px 0;
            padding: 14px 18px;
            border-radius: 14px;
            border: 1px solid rgba(148,163,184,0.35);
            background: radial-gradient(circle at top left, rgba(15,23,42,0.95), rgba(15,23,42,0.85));
            box-shadow: 0 16px 40px rgba(15,23,42,0.9);
            text-align: center;
        ">
            <a href="{login_url}"
               style="
                   font-size: 1rem;
                   font-weight: 700;
                   color: #38bdf8;
                   text-decoration: none;
               ">
                🔐 Log in with Auth0
            </a>
        </div>
        """,
        unsafe_allow_html=True
    )

    # 🏀 Games list
    render_landing_nba_games()

    st.stop()

# ------------------------------------------------------
# REQUIRE LOGIN
# ------------------------------------------------------
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


        /* ---------- GAME CARD (separate + conflict-free) ---------- */

    .game-card {{
        padding:18px 22px;
        margin-bottom:22px;
        border-radius:20px;
        border:1px solid rgba(148,163,184,0.28);
        background: radial-gradient(circle at top left, rgba(30,41,59,1), rgba(15,23,42,0.92));
        box-shadow:0 22px 55px rgba(15,23,42,0.90);
    }}

    .game-headline {{
        display:flex;
        justify-content:space-between;
        align-items:flex-start;
        margin-bottom:14px;
    }}

    .game-team {{
        font-size:1.05rem;
        font-weight:700;
        color:white;
    }}

    .game-metric {{
        font-size:0.88rem;
        color:#e5e7eb;
        margin-top:4px;
    }}

    .game-pill {{
        background:rgba(255,255,255,0.08);
        border:1px solid rgba(255,255,255,0.18);
        padding:6px 12px;
        border-radius:12px;
        font-size:0.85rem;
        color:#e5e7eb;
        margin-top:4px;
    }}

    .game-row {{
        display:flex;
        justify-content:space-between;
        gap:20px;
        margin-top:12px;
    }}

    .game-col {{
        flex:1;
    }}


    </style>
    """,
    unsafe_allow_html=True,
)


st.markdown("""
<style>

.card-tap-btn .stButton > button {
    all: unset !important;
    display: block !important;
    width: 100% !important;
    height: 50px !important;      /* tap area */
    cursor: pointer !important;
    background: transparent !important;
}

/* No visual change on hover/focus/active */
.card-tap-btn .stButton > button:hover,
.card-tap-btn .stButton > button:focus,
.card-tap-btn .stButton > button:active {
    all: unset !important;
    display: block !important;
    width: 100% !important;
    height: 50px !important;
}

</style>
""", unsafe_allow_html=True)

components.html("""
<style>
.ncaab-card-container {
    background: #111;
    border-radius: 14px;
    padding: 16px;
    margin-bottom: 16px;
    border: 1px solid rgba(255,255,255,0.1);
    color: #eee;
    font-family: Inter, sans-serif;
}
.ncaab-card-header {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 14px;
    margin-bottom: 10px;
}
.ncaab-team-logo {
    width: 50px;
    height: 50px;
    object-fit: contain;
}
.ncaab-vs-text {
    font-size: 1.25rem;
    font-weight: 700;
    color: #999;
}
.ncaab-team-names {
    display: flex;
    justify-content: space-between;
    padding: 0 10px;
    font-size: 0.95rem;
    margin-bottom: 8px;
}
.ncaab-start {
    text-align: center;
    color: #999;
    font-size: 0.85rem;
    margin-bottom: 6px;
}
.ncaab-score {
    margin-top: 8px;
    text-align: center;
    font-size: 1.1rem;
    font-weight: 700;
}
</style>
""", height=0)



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
    ["NBA", "NCAA Men's"],
    index=0,
)


# ------------------------------------------------------
# HEADER
# ------------------------------------------------------
st.title("Pulse Sports Analytics")


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

def team_abbr(team_name: str) -> str:
    """
    Returns 3-letter NBA abbreviation.
    Falls back safely if name not found.
    """
    return TEAM_NAME_TO_CODE.get(team_name, team_name[:3].upper())

def logo(team_name: str) -> str:
    code = TEAM_NAME_TO_CODE.get(team_name)
    if not code:
        return "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"
    return TEAM_LOGOS.get(code)

def fmt(
    val,
    decimals: int = 1,
    plus: bool = False,
    pct: bool = False,
):
    """
    Format numeric values safely for UI display.

    Examples:
    fmt(113.72)            -> '113.7'
    fmt(-3.2, plus=True)   -> '-3.2'
    fmt(3.2, plus=True)    -> '+3.2'
    fmt(0.598, pct=True)   -> '59.8%'
    """

    if val is None:
        return "—"

    try:
        val = float(val)
    except Exception:
        return "—"

    if pct:
        return f"{val * 100:.{decimals}f}%"

    if plus:
        return f"{val:+.{decimals}f}"

    return f"{val:.{decimals}f}"

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

NO_IMAGE = "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"

import pandas as pd

def ncaa_logo(espn_team_id):
    """
    Safe ESPN logo resolver.
    Accepts int, str, or missing values.
    """

    # Handle missing / NaN / NA
    if espn_team_id is None or pd.isna(espn_team_id):
        return "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"

    try:
        tid = int(espn_team_id)
    except (ValueError, TypeError):
        return "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"

    return f"https://a.espncdn.com/i/teamlogos/ncaa/500/{tid}.png"


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

# Prop card accordion (only one open at a time)
if "open_prop_card" not in st.session_state:
    st.session_state.open_prop_card = None


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

# ------------------------------------------------------
# LOAD NCAA GAME ANALYTICS
# ------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_ncaab_game_analytics():
    df = bq_client.query(NCAAB_GAME_ANALYTICS_SQL).to_dataframe()
    df.columns = df.columns.str.strip()

    # Datetime normalization
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")

    # Numeric normalization
    numeric_cols = [
        "home_ml", "away_ml",
        "home_spread", "away_spread",
        "total_line",
        "proj_home_points", "proj_away_points", "proj_total_points",
        "proj_margin",
        "spread_edge", "total_edge",
        "l5_scoring_diff", "l10_scoring_diff",
        "l5_margin_diff", "l10_margin_diff",
        "pace_proxy"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

@st.cache_data(show_spinner=True)
def load_game_report():
    df = bq_client.query(GAME_REPORT_SQL).to_dataframe()
    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df

@st.cache_data(ttl=300)
def load_game_odds() -> pd.DataFrame:
    df = bq_client.query(GAME_ODDS_SQL).to_dataframe()

    # Ensure snake_case columns exist and are typed correctly
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

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
ncaab_game_analytics_df = load_ncaab_game_analytics()


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

        # VALUES
        "pts_last5_list", "pts_last7_list", "pts_last10_list",
        "reb_last5_list", "reb_last7_list", "reb_last10_list",
        "ast_last5_list", "ast_last7_list", "ast_last10_list",
        "stl_last5_list", "stl_last7_list", "stl_last10_list",
        "blk_last5_list", "blk_last7_list", "blk_last10_list",
        "pra_last5_list", "pra_last7_list", "pra_last10_list",
        "pr_last5_list",  "pr_last7_list",  "pr_last10_list",
        "pa_last5_list",  "pa_last7_list",  "pa_last10_list",
        "ra_last5_list",  "ra_last7_list",  "ra_last10_list",

        # ✅ DATES (NEW)
        "last5_dates",
        "last7_dates",
        "last10_dates",
        "last20_dates",
    ]]
)


# Merge into props (so card_df rows have all lists)
props_df = props_df.merge(hist_latest, on="player_norm", how="left")
# 🔥 ENSURE sparkline list columns are real Python lists
props_df = convert_list_columns(props_df)



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

# ==========================================
# EXPANDER TOGGLE HELPER (required for Mode C)
# ==========================================
def toggle_expander(key: str):
    if key not in st.session_state:
        st.session_state[key] = True
    else:
        st.session_state[key] = not st.session_state[key]
        
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

def get_spark_series(row):
    stat = detect_stat(row.get("market", ""))
    if not stat:
        return [], []

    candidates = [
        (f"{stat}_last10_list", "last10_dates"),
        (f"{stat}_last7_list",  "last7_dates"),
        (f"{stat}_last5_list",  "last5_dates"),
    ]

    for val_col, date_col in candidates:
        vals = row.get(val_col)
        dates = row.get(date_col)

        # Skip if either side is missing
        if vals is None or dates is None:
            continue

        # Coerce to plain Python lists (handles list, ndarray, Series)
        try:
            vals = list(vals)
            dates = list(dates)
        except Exception:
            continue

        clean = []
        for v, d in zip(vals, dates):
            if isinstance(v, (int, float)):
                clean.append((v, d))

        if not clean:
            continue

        values, raw_dates = zip(*clean)

        fmt_dates = []
        for d in raw_dates:
            try:
                fmt_dates.append(pd.to_datetime(d).strftime("%m/%d"))
            except Exception:
                fmt_dates.append("")

        return list(values), fmt_dates


    return [], []


def build_sparkline_bars_hitmiss(
    values,
    dates,
    line_value,
    width=110,
    height=46
):
    if not values:
        return ""

    n = len(values)
    bar_width = width / n

    max_v = max(max(values), line_value)
    min_v = min(min(values), line_value)
    span = (max_v - min_v) or 1

    rects, labels, date_labels = [], [], []

    for i, (v, d) in enumerate(zip(values, dates)):
        bar_height = (v - min_v) / span * (height - 18)
        x = i * bar_width
        y = height - 14 - bar_height

        color = "#22c55e" if v >= line_value else "#ef4444"

        rects.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" '
            f'width="{bar_width - 2:.1f}" height="{bar_height:.1f}" '
            f'fill="{color}" rx="2" />'
        )

        labels.append(
            f'<text x="{x + bar_width/2:.1f}" y="{y - 2:.1f}" '
            f'font-size="6px" fill="#e5e7eb" text-anchor="middle">{int(v)}</text>'
        )

        # ⬇️ DATE LABEL
        date_labels.append(
            f'<text x="{x + bar_width/2:.1f}" y="{height - 4}" '
            f'font-size="7px" fill="#9ca3af" text-anchor="middle" '
            f'transform="rotate(-90 {x + bar_width/2:.1f} {height - 4})">{d}</text>'
        )



    line_y = height - 14 - ((line_value - min_v) / span * (height - 18))
    line_elem = (
        f'<line x1="0" y1="{line_y:.1f}" '
        f'x2="{width}" y2="{line_y:.1f}" '
        f'stroke="#9ca3af" stroke-width="1" stroke-dasharray="3,2" />'
    )

    return f"""
    <svg width="{width}" height="{height}">
        {''.join(rects)}
        {''.join(labels)}
        {line_elem}
        {''.join(date_labels)}
    </svg>
    """


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

def pretty_game_time(dt):
    """
    Format datetime as: Thu, Dec 11 • 6:30 PM ET
    """
    if not dt:
        return ""

    import pytz
    from datetime import datetime

    try:
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        return str(dt)

    est = pytz.timezone("America/New_York")
    dt_est = dt.astimezone(est)

    return dt_est.strftime("%a, %b %d • %-I:%M %p ET")

# --------------------------------------------------------
# NCAA MEN'S — RENDER GAME OVERVIEW CARD (FINAL VERSION)
# --------------------------------------------------------
import streamlit.components.v1 as components
from datetime import datetime
import pytz

def fmt1(x):
    """Format float with 1 decimal, or '-' if None."""
    try:
        return f"{float(x):.1f}"
    except:
        return "-"

def render_ncaab_overview_card(row):

    home = row.get("home_team", "")
    away = row.get("away_team", "")

    home_logo = ncaa_logo(row["home_espn_team_id"])
    away_logo = ncaa_logo(row["away_espn_team_id"])

    # ----------------------------------------
    # CORRECT FIELD NAMES FROM GAME ANALYTICS
    # ----------------------------------------
    exp_home = row.get("proj_home_points")
    exp_away = row.get("proj_away_points")
    exp_total = row.get("proj_total_points")
    exp_spread = row.get("proj_margin")  # home - away predicted margin

    # ------------------------
    # Pretty Time Conversion
    # ------------------------
    dt = row.get("start_time")
    pretty_time = ""
    if dt:
        if not isinstance(dt, datetime):
            dt = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))

        est = pytz.timezone("America/New_York")
        dt_est = dt.astimezone(est)
        pretty_time = dt_est.strftime("%a, %b %d • %I:%M %p ET")

    # ------------------------
    # HTML CARD
    # ------------------------
    html = f"""
    <div style="
        width:100%;
        background:rgba(255,255,255,0.06);
        border:1px solid rgba(255,255,255,0.12);
        border-radius:14px;
        padding:14px 14px;
        margin-bottom:12px;
        color:#e5e7eb;
        font-family:Inter, sans-serif;
    ">

        <!-- Logos Row -->
        <div style="
            display:flex;
            justify-content:center;
            align-items:center;
            gap:20px;
            margin-bottom:8px;
        ">
            <img src="{away_logo}" style="height:56px; width:auto;" />
            <span style="font-size:1.15rem; font-weight:700;">VS</span>
            <img src="{home_logo}" style="height:56px; width:auto;" />
        </div>

        <!-- Team Names -->
        <div style="
            display:flex;
            justify-content:space-between;
            margin-bottom:6px;
            font-size:0.95rem;
            font-weight:700;
        ">
            <div style="flex:1; text-align:center;">{away}</div>
            <div style="flex:1; text-align:center;">{home}</div>
        </div>

        <!-- Expected Points -->
        <div style="
            display:flex;
            justify-content:space-between;
            margin-bottom:6px;
            font-size:0.9rem;
            color:#d1d5db;
        ">
            <div style="flex:1; text-align:center;">Exp: {fmt1(exp_away)}</div>
            <div style="flex:1; text-align:center;">Exp: {fmt1(exp_home)}</div>
        </div>

        <!-- Spread & Total -->
        <div style="
            text-align:center;
            margin-bottom:6px;
            font-size:0.9rem;
        ">
            Spread: {fmt1(exp_spread)} • Total: {fmt1(exp_total)}
        </div>

        <!-- Pretty Start Time -->
        <div style="
            text-align:center;
            font-size:0.85rem;
            color:#9ca3af;
        ">
            {pretty_time}
        </div>

    </div>
    """


    components.html(html, height=280, scrolling=False)

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
    Mobile-safe prop cards with single-card expansion (Option A).
    Cards scroll; expanded analytics render outside scroll container.
    """

    if df.empty:
        st.info("No props match your filters.")
        return

    # ------------------------------------------------------
    # WOWY merge once per render
    # ------------------------------------------------------
    card_df = attach_wowy_deltas(df, wowy_df)

    # ------------------------------------------------------
    # Filtering logic
    # ------------------------------------------------------
    def card_good(row):
        price = row.get("price")
        hit = row.get(hit_rate_col)

        if price is None or hit is None:
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

    card_df = card_df[card_df.apply(card_good, axis=1)].reset_index(drop=True)

    if card_df.empty:
        st.info("No props match your filters.")
        return

    # ------------------------------------------------------
    # Sorting
    # ------------------------------------------------------
    card_df = card_df.sort_values(
        by=[hit_rate_col, "price"],
        ascending=[False, True],
    ).reset_index(drop=True)

    # ------------------------------------------------------
    # Pagination
    # ------------------------------------------------------
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
        key=f"{page_key}_page",
    )

    start = (page - 1) * page_size
    end = start + page_size
    page_df = card_df.iloc[start:end]

    # ------------------------------------------------------
    # Track expanded card
    # ------------------------------------------------------
    expanded_row = None
    expanded_card_id = None

    # ======================================================
    # SCROLL WRAPPER (CARDS ONLY)
    # ======================================================
    st.markdown(
        "<div style='max-height:1100px;overflow-y:auto;padding-right:12px;'>",
        unsafe_allow_html=True,
    )

    cols = st.columns(4)

    for idx, row in page_df.iterrows():
        col = cols[idx % 4]

        player = row.get("player", "")
        market = row.get("market", "")
        line = row.get("line", "")
        card_id = f"{page_key}_{idx}_{player}_{market}_{line}"

        with col:
            # --------------------------------------------------
            # CARD HTML (strict f-strings)
            # --------------------------------------------------
            card_lines = [
                '<div class="prop-card">',
                ...
                '</div>',
            ]
            card_html = "\n".join(card_lines)

            st.markdown(card_html, unsafe_allow_html=True)

            # --------------------------------------------------
            # TAP BUTTON
            # --------------------------------------------------
            tapped = st.button("tap", key=f"{card_id}_tap")

            if tapped:
                if st.session_state.get("open_prop_card") == card_id:
                    st.session_state.open_prop_card = None
                else:
                    st.session_state.open_prop_card = card_id

            if st.session_state.get("open_prop_card") == card_id:
                expanded_row = row.copy()
                expanded_card_id = card_id

    # ======================================================
    # CLOSE SCROLL WRAPPER
    # ======================================================
    st.markdown("</div>", unsafe_allow_html=True)

    # ======================================================
    # EXPANDED SECTION (FULL WIDTH, MOBILE SAFE)
    # ======================================================
    if expanded_row is None:
        return

    row = expanded_row
    card_id = expanded_card_id

    stat = detect_stat(row.get("market", "")) or ""

    # ---- safe values ----
    l5 = row.get(f"{stat}_last5") if stat else None
    l10 = row.get(f"{stat}_last10") if stat else None
    l20 = row.get(f"{stat}_last20") if stat else None
    delta_vs_line = row.get("proj_diff_vs_line") or 0
    delta_color = "#22c55e" if delta_vs_line > 0 else "#ef4444"

    # ---- open shell ----
    st.markdown(
        "<div style='margin:12px 0;padding:16px;background:rgba(15,23,42,0.97);"
        "border:1px solid rgba(148,163,184,0.25);border-radius:16px;'>",
        unsafe_allow_html=True,
    )

    # ---- context strip ----
    st.markdown(
        f"<div style='display:flex;flex-wrap:wrap;gap:12px;"
        f"font-size:0.75rem;color:#cbd5f5;margin-bottom:14px;'>"
        f"<div>🕒 {pretty_game_time(row.get('start_time'))}</div>"
        f"<div>⏱ {fmt(row.get('est_minutes'))} min</div>"
        f"<div>📈 Usage {fmt(row.get('usage_bump_pct'), pct=True)}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ---- L5/L10/L20/Δ ----
    st.markdown(
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:10px;"
        f"margin-bottom:16px;text-align:center;'>"
        f"<div><div style='font-size:0.65rem;color:#9ca3af;'>L5</div>"
        f"<div style='font-size:1.05rem;font-weight:800;'>{fmt(l5)}</div></div>"
        f"<div><div style='font-size:0.65rem;color:#9ca3af;'>L10</div>"
        f"<div style='font-size:1.05rem;font-weight:800;'>{fmt(l10)}</div></div>"
        f"<div><div style='font-size:0.65rem;color:#9ca3af;'>L20</div>"
        f"<div style='font-size:1.05rem;font-weight:800;'>{fmt(l20)}</div></div>"
        f"<div><div style='font-size:0.65rem;color:#9ca3af;'>Δ Line</div>"
        f"<div style='font-size:1.05rem;font-weight:900;color:{delta_color};'>"
        f"{fmt(delta_vs_line, plus=True)}</div></div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ---- sparkline (last 20) ----
    vals = row.get(f"{stat}_last20_list")
    dates = row.get("last20_dates")

    if isinstance(vals, list) and isinstance(dates, list) and vals:
        try:
            spark_html = build_sparkline_bars_hitmiss(
                values=vals[-20:],
                dates=[str(d)[5:] for d in dates[-20:]],
                line_value=float(row.get("line") or 0),
                width=280,
                height=64,
            )
            st.markdown(
                "<div style='font-size:0.75rem;color:#9ca3af;margin-bottom:6px;'>"
                "Last 20 Games</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='display:flex;justify-content:center;'>{spark_html}</div>",
                unsafe_allow_html=True,
            )
        except Exception:
            pass

    # ---- WOWY best impact ----
    delta_col = market_to_delta_column(row.get("market"))
    wrows = row.get("_wowy_list") or []

    if delta_col and wrows:
        best = max(
            wrows,
            key=lambda x: abs(x.get(delta_col) or 0),
            default=None,
        )
        if best and best.get(delta_col) is not None:
            dv = float(best.get(delta_col))
            c = "#22c55e" if dv > 0 else "#ef4444"
            st.markdown(
                f"<div style='margin-top:14px;padding:12px 14px;"
                f"border-left:4px solid {c};background:rgba(255,255,255,0.04);"
                f"border-radius:12px;'>"
                f"<div style='font-size:0.78rem;font-weight:900;color:{c};'>"
                f"{dv:+.2f} {stat.upper()} WOWY</div>"
                f"<div style='font-size:0.75rem;color:#cbd5f5;margin-top:2px;'>"
                f"{best.get('breakdown','')}</div></div>",
                unsafe_allow_html=True,
            )

    # ---- actions ----
    st.markdown(
        "<hr style='margin:16px 0;border:none;"
        "border-top:1px solid rgba(148,163,184,0.18);'>",
        unsafe_allow_html=True,
    )

    if st.button("💾 Save Bet", key=f"{card_id}_save"):
        save_bet_for_user(
            user_id,
            {
                "player": row.get("player"),
                "market": row.get("market"),
                "line": row.get("line"),
                "bet_type": row.get("bet_type"),
                "price": row.get("price"),
                "bookmaker": row.get("bookmaker"),
            },
        )
        st.success("Saved to Betslip")

    if st.button("📊 Open Trend Lab", key=f"{card_id}_trend"):
        st.session_state.trend_player = row.get("player")
        st.session_state.trend_market = row.get("market")
        st.session_state.trend_line = row.get("line")
        st.session_state.trend_bet_type = row.get("bet_type")
        st.toast("Sent to Trend Lab")

    # ---- close shell ----
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
    tab1, tab2, tab3, tab4, tab7, tab8 = st.tabs(
        [
            "📈 Props",
            "🏀 Game Lines",
            "📊 Advanced EV Tools",
            "📊 Player Context Hub",
            "📈 Trend Lab",
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

    # ======================================================
    # 🏀 TAB 2 — GAME LINES + MODEL EV (ML · SPREAD · TOTAL)
    # ======================================================
    with tab2:

        # ✅ CRITICAL: bind tab2 to game_report
        df = game_report_df.copy()
    
        st.subheader("🏀 Game Lines + Model EV (ML · Spread · Total)")
    
        # --------------------------------------------------
        # Guard: no games
        # --------------------------------------------------
        if df.empty:
            st.info("No games available.")
            st.stop()
    
        # ==============================================
        # EXPANDABLE GAME CARD RENDERER (FIXED)
        # ==============================================
        def render_game_card(
            game_id,
            home, away, start_time,
            home_logo, away_logo,
            home_pts, away_pts,
            home_win, away_win,
            tot_pts, margin,
            pace, pace_delta,
            home_l5, away_l5,
            home_ml_text, away_ml_text,
            spread_text, total_text,
        ):
        
            home_abbr = team_abbr(home)
            away_abbr = team_abbr(away)
            
            # --- COLLAPSED (compact, NO EV) ---
            
            # Moneyline: take just the odds number
            collapsed_home_ml = f"{home_abbr} {home_ml_text.split()[1]}"
            collapsed_away_ml = f"{away_abbr} {away_ml_text.split()[1]}"
            
            # Spread: keep the number, replace team name with abbr
            collapsed_spread = f"{home_abbr} {spread_text.split()[-1]}"
            
            # Total: strip text down to O/U number
            collapsed_total = f"O/U {total_text.replace('O/U', '').strip()}"
            
            html = f"""
            <style>
            .game-card {{
                background: linear-gradient(145deg, #0f172a, #1e293b);
                border-radius: 18px;
                padding: 16px;
                margin-bottom: 10px;
                color: white;
                border: 1px solid rgba(255,255,255,0.06);
                transition: 0.15s ease-in-out;
            }}
            
            .game-card.expanded-card {{
                margin-bottom: 18px;
            }}
            
            .game-card:hover {{
                background: linear-gradient(145deg, #162236, #253348);
            }}
            
            .expand-section {{
                max-height: 0;
                overflow: hidden;
                transition: max-height 0.35s ease;
            }}
            
            .expand-section.expanded {{
                max-height: 900px;
                margin-top: 8px;
            }}
            
            .expand-hint {{
                color: #94a3b8;
                font-size: 0.8rem;
                margin-top: 10px;
                text-align: center;
            }}
            
            .team-col {{
                text-align: center;
                width: 120px;
            }}
            
            .proj-pts {{
                margin-top: 4px;
                font-size: 0.9rem;
                color: #93c5fd;
                font-weight: 600;
            }}
            
            .center-col {{
                text-align: center;
                margin-top: 6px;
                min-width: 80px;
            }}
            
            .section-box {{
                background: rgba(255,255,255,0.06);
                padding: 12px;
                border-radius: 12px;
                font-size: 0.85rem;
            }}
            
            .section-title {{
                color: #94a3b8;
                font-size: 0.8rem;
                margin-bottom: 6px;
            }}
            
            /* -------- COLLAPSED SUMMARY -------- */
            
            .summary-row {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 12px;
                margin-top: 10px;
                padding-top: 8px;
                border-top: 1px solid rgba(255,255,255,0.08);
                font-size: 0.75rem;
            }}
            
            .summary-col {{
                display: grid;
                row-gap: 4px;
            }}
            
            .summary-title {{
                color: #94a3b8;
                font-size: 0.65rem;
                letter-spacing: 0.05em;
                text-transform: uppercase;
                margin-bottom: 2px;
            }}
            
            .summary-line {{
                display: grid;
                grid-template-columns: auto 1fr;
                column-gap: 6px;
                align-items: center;
            }}
            
            .summary-label {{
                color: #94a3b8;
                white-space: nowrap;
            }}
            
            .summary-value {{
                color: #e5e7eb;
                font-weight: 600;
                text-align: right;
                white-space: nowrap;
            }}
            </style>
            
            <div class="game-card" onclick="toggleExpand('{game_id}')">
            
                <!-- HEADER -->
                <div style="display:flex;justify-content:space-between;align-items:flex-start;">
            
                    <!-- HOME -->
                    <div class="team-col">
                        <img src="{home_logo}" width="42" style="border-radius:6px;">
                        <div style="font-weight:700;margin-top:4px;">{home}</div>
                        <div class="proj-pts">{home_pts} pts</div>
                    </div>
            
                    <!-- CENTER -->
                    <div class="center-col">
                        <div style="font-size:1.25rem;font-weight:700;color:#e5e7eb;">vs</div>
                        <div style="margin-top:4px;font-size:0.85rem;color:#9ca3af;">
                            {start_time}
                        </div>
                    </div>
            
                    <!-- AWAY -->
                    <div class="team-col">
                        <img src="{away_logo}" width="42" style="border-radius:6px;">
                        <div style="font-weight:700;margin-top:4px;">{away}</div>
                        <div class="proj-pts">{away_pts} pts</div>
                    </div>
            
                </div>
            
                <!-- COLLAPSED SUMMARY -->
                <div class="summary-row">
                
                    <!-- MODEL -->
                    <div class="summary-col">
                        <div class="summary-title">Model</div>
                
                        <div class="summary-line">
                            <span class="summary-label">Total</span>
                            <span class="summary-value">{tot_pts}</span>
                        </div>
                
                        <div class="summary-line">
                            <span class="summary-label">Spread</span>
                            <span class="summary-value">{margin}</span>
                        </div>
                
                        <div class="summary-line">
                            <span class="summary-label">ML</span>
                            <span class="summary-value">{home_win}%–{away_win}%</span>
                        </div>
                    </div>
                
                    <!-- MARKET -->
                    <div class="summary-col">
                        <div class="summary-title">Market</div>
                
                        <div class="summary-line">
                            <span class="summary-label">Total</span>
                            <span class="summary-value">{collapsed_total}</span>
                        </div>
                
                        <div class="summary-line">
                            <span class="summary-label">Spread</span>
                            <span class="summary-value">{collapsed_spread}</span>
                        </div>
                
                        <div class="summary-line">
                            <span class="summary-label">ML</span>
                            <span class="summary-value">
                                {collapsed_home_ml} / {collapsed_away_ml}
                            </span>
                        </div>
                    </div>
                
                </div>
            
                <div class="expand-hint">Tap to expand ↓</div>
            
                <!-- EXPANDED -->
                <div id="expand-{game_id}" class="expand-section">
                    <div style="margin-top:16px;padding-top:14px;border-top:1px solid rgba(255,255,255,0.08);">
            
                        <div style="display:flex;gap:18px;flex-wrap:wrap;">
            
                            <!-- MODEL -->
                            <div style="flex:1;min-width:220px;">
                                <div class="section-title">Model Projections</div>
                                <div class="section-box">
                                    Win %: {home_win}% / {away_win}%<br>
                                    Projected Total: <b>{tot_pts}</b><br>
                                    Model Spread: <b>{margin}</b><br>
                                    Pace: {pace} ({pace_delta})<br>
                                    L5 Diff: {home_l5} / {away_l5}
                                </div>
                            </div>
            
                            <!-- MARKET -->
                            <div style="flex:1;min-width:220px;">
                                <div class="section-title">Market Lines</div>
                                <div class="section-box">
                                    <b>Moneyline</b><br>
                                    {home_ml_text}<br>
                                    {away_ml_text}<br><br>
            
                                    <b>Spread</b><br>
                                    {spread_text}<br><br>
            
                                    <b>Total</b><br>
                                    {total_text}
                                </div>
                            </div>
            
                        </div>
            
                    </div>
                </div>
            </div>
            
            <script>
            function toggleExpand(id) {{
                var section = document.getElementById("expand-" + id);
                var card = section.closest(".game-card");
            
                section.classList.toggle("expanded");
                card.classList.toggle("expanded-card");
            }}
            </script>
            """
            components.html(html, height=260)
    
        # ===============================================
        # RENDER GAME CARDS
        # ===============================================
        for _, row in df.iterrows():
    
            home = row["home_team"]
            away = row["visitor_team"]
            
            # ---------------------------
            # START TIME (SAFE)
            # ---------------------------
            start_time = row.get("start_time_formatted") or row.get("start_time_est", "")
    
            if hasattr(start_time, "strftime"):
                start_time = start_time.strftime("%-I:%M %p ET")
            
            game_id = (
                f"{home}_{away}"
                .lower()
                .replace(" ", "")
                .replace(".", "")
                .replace("-", "")
            )
    
            home_logo = logo(home)
            away_logo = logo(away)
            home_abbr = team_abbr(home)
            away_abbr = team_abbr(away)
    
            home_pts = fmt(row.get("exp_home_points"))
            away_pts = fmt(row.get("exp_visitor_points"))
            tot_pts = fmt(row.get("exp_total_points"))
            margin = fmt(row.get("predicted_margin"), plus=True)
            home_win = fmt(row.get("home_win_pct"))
            away_win = fmt(row.get("visitor_win_pct"))
            pace = fmt(row.get("pace_proxy"))
            pace_delta = fmt(row.get("pace_delta"), plus=True)
            home_l5 = fmt(row.get("home_l5_diff"), plus=True)
            away_l5 = fmt(row.get("visitor_l5_diff"), plus=True)
    
            # --------------------------------------------------
            # DEFAULT ODDS (ALWAYS DEFINED)
            # --------------------------------------------------
            home_ml_text = "No ML odds"
            away_ml_text = ""
            spread_text = "No spread odds"
            total_text = "No total odds"
    
            # --------------------------------------------------
            # GAME ODDS + MODEL OUTPUT (FROM game_report)
            # --------------------------------------------------
            
            # ------------------------------------------------------
            # MONEYLINE
            # ------------------------------------------------------
            home_ml = row.get("home_ml")
            away_ml = row.get("visitor_ml")
            
            home_ml_edge = row.get("home_ml_edge")
            away_ml_edge = row.get("visitor_ml_edge")
            
            # --- EXPANDED (verbose, with EV) ---
            home_ml_text = (
                f"{home}: <b>{home_ml:+}</b> "
                f"<span style='opacity:0.7'>EV {home_ml_edge:+.2f}%</span>"
                if pd.notna(home_ml) else "—"
            )
            
            away_ml_text = (
                f"{away}: <b>{away_ml:+}</b> "
                f"<span style='opacity:0.7'>EV {away_ml_edge:+.2f}%</span>"
                if pd.notna(away_ml) else "—"
            )
            
            # --- COLLAPSED (compact, NO EV) ---
            collapsed_ml_lines = (
                [
                    f"{home_abbr} {home_ml:+}",
                    f"{away_abbr} {away_ml:+}",
                ]
                if pd.notna(home_ml) and pd.notna(away_ml)
                else ["—"]
            )
            
            
            # ------------------------------------------------------
            # SPREAD
            # ------------------------------------------------------
            home_spread = row.get("home_spread")
            visitor_spread = row.get("visitor_spread")
            
            home_spread_price = row.get("home_spread_price")
            visitor_spread_price = row.get("visitor_spread_price")
            
            home_spread_edge = row.get("home_spread_edge")
            visitor_spread_edge = row.get("visitor_spread_edge")
            
            # --- EXPANDED (verbose, with EV) ---
            spread_text = (
                f"{home} {home_spread:+.1f} "
                f"(<b>{home_spread_price:+}</b>, "
                f"EV {home_spread_edge:+.2f}%)"
                if pd.notna(home_spread) else "—"
            )
            
            # --- COLLAPSED (compact, NO EV) ---
            collapsed_spread_lines = (
                [
                    f"{home_abbr} {home_spread:+.1f} {home_spread_price:+}",
                    f"{away_abbr} {visitor_spread:+.1f} {visitor_spread_price:+}",
                ]
                if (
                    pd.notna(home_spread)
                    and pd.notna(visitor_spread)
                    and pd.notna(home_spread_price)
                    and pd.notna(visitor_spread_price)
                )
                else ["—"]
            )
            
            
            # ------------------------------------------------------
            # TOTAL (OVER / UNDER)
            # ------------------------------------------------------
            total_line = row.get("total_line")
            over_price = row.get("total_price")                 # Over price
            under_price = row.get("visitor_total_price")        # Under price
            
            total_edge = row.get("total_edge_pts")
            
            # --- EXPANDED (verbose, with EV) ---
            total_text = (
                f"O/U {total_line:.1f} "
                f"(<b>{over_price:+}</b>, "
                f"EV {total_edge:+.2f} pts)"
                if pd.notna(total_line) else "—"
            )
            
            # --- COLLAPSED (compact, NO EV) ---
            collapsed_total_lines = (
                [
                    f"O {over_price:+}",
                    f"U {under_price:+}",
                ]
                if (
                    pd.notna(total_line)
                    and pd.notna(over_price)
                    and pd.notna(under_price)
                )
                else ["—"]
            )
    
            # --------------------------------------------------
            # RENDER CARD
            # --------------------------------------------------
            render_game_card(
                game_id,
                home, away, start_time,
                home_logo, away_logo,
                home_pts, away_pts,
                home_win, away_win,
                tot_pts, margin,
                pace, pace_delta,
                home_l5, away_l5,
                home_ml_text, away_ml_text,
                spread_text, total_text
            )

    # ------------------------------------------------------
    # NEW COMBINED TAB — ADVANCED EV TOOLS
    # ------------------------------------------------------
    with tab3:

        st.subheader("📊 Advanced EV Tools")

        # --- Create subtabs ---
        subtab1, subtab2, subtab3 = st.tabs(
            [
                "🏅 EV Leaderboard",
                "🗺️ EV Heatmap",
                "📐 Trend Projection Model",
            ]
        )

        # ======================================================
        # 🏅 SUBTAB 1 — EV LEADERBOARD
        # ======================================================
        with subtab1:
            st.markdown("### 🏅 EV Leaderboard")

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

                # Filter on edge + hit rate
                if "edge_pct" in df_leader.columns:
                    df_leader = df_leader[df_leader["edge_pct"] >= min_edge / 100.0]
                if "hit_rate_last10" in df_leader.columns:
                    df_leader = df_leader[df_leader["hit_rate_last10"] >= min_hit / 100.0]

                # Sorting
                sort_cols = [c for c in ["edge_pct", "ev_last10"] if c in df_leader.columns]
                if sort_cols:
                    df_leader = df_leader.sort_values(sort_cols, ascending=False)

                df_leader["market_pretty"] = df_leader["market"].map(
                    lambda m: MARKET_DISPLAY_MAP.get(m, m)
                )

                cols_to_show = [
                    "player", "market_pretty", "bet_type", "line", "price",
                    "hit_rate_last10", "implied_prob", "edge_pct", "ev_last10",
                    "proj_last10", "proj_diff_vs_line", "matchup_difficulty_score",
                    "est_minutes", "usage_bump_pct",
                ]
                cols_to_show = [c for c in cols_to_show if c in df_leader.columns]

                if df_leader.empty:
                    st.info("No props meet the current leaderboard filters.")
                else:
                    display_df = df_leader[cols_to_show].copy()

                    if "price" in display_df.columns:
                        display_df["price"] = display_df["price"].apply(format_moneyline)
                    if "hit_rate_last10" in display_df.columns:
                        display_df["hit_rate_last10"] = (display_df["hit_rate_last10"] * 100).round(1)
                    if "implied_prob" in display_df.columns:
                        display_df["implied_prob"] = (display_df["implied_prob"] * 100).round(1)
                    if "edge_pct" in display_df.columns:
                        display_df["edge_pct"] = display_df["edge_pct"].round(1)

                    st.dataframe(display_df, use_container_width=True, hide_index=True)

        # ======================================================
        # 🗺️ SUBTAB 2 — EV HEATMAP
        # ======================================================
        with subtab2:
            st.markdown("### 🗺️ EV Stat vs Opponent Heatmap")

            if props_df.empty:
                st.info("No props available.")
            else:
                heat_df = props_df.copy()
                heat_df["stat_key"] = heat_df["market"].apply(detect_stat)

                if "edge_pct" not in heat_df.columns:
                    st.warning("edge_pct column missing — cannot build heatmap.")
                else:
                    heat_df["edge_pct"] = pd.to_numeric(heat_df["edge_pct"], errors="coerce")
                    heat_df = heat_df[
                        heat_df["stat_key"].notna()
                        & heat_df["opponent_team"].notna()
                        & heat_df["edge_pct"].notna()
                    ]

                    if heat_df.empty:
                        st.info("Not enough data for heatmap.")
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

                        fig.update_traces(
                            hovertemplate="Stat: %{y}<br>Opponent: %{x}<br>Edge: %{z:.1f}%%<extra></extra>"
                        )

                        st.plotly_chart(fig, use_container_width=True)

        # ======================================================
        # 📐 SUBTAB 3 — TREND PROJECTION MODEL
        # ======================================================
        with subtab3:
            st.markdown("### 📐 Trend-Based Projection Model")

            if props_df.empty:
                st.info("No props available.")
            else:
                proj_df = props_df.copy()

                needed = [
                    "proj_last10", "proj_std_last10", "proj_volatility_index",
                    "proj_diff_vs_line", "hit_rate_last10", "price",
                ]
                for c in needed:
                    if c in proj_df.columns:
                        proj_df[c] = pd.to_numeric(proj_df[c], errors="coerce")

                c1, c2, c3 = st.columns(3)
                with c1:
                    min_proj_diff = st.slider("Min Projection vs Line", -10.0, 20.0, 1.0, 0.5)
                with c2:
                    max_vol_index = st.slider("Max Volatility Index", 0.0, 5.0, 3.0, 0.1)
                with c3:
                    min_hit10_proj = st.slider("Min Hit Rate L10 (%)", 0, 100, 50, 5)

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
                    "player", "market_pretty", "bet_type", "line", "price",
                    "proj_last10", "proj_std_last10", "proj_volatility_index",
                    "proj_diff_vs_line", "hit_rate_last10", "edge_pct",
                ]
                cols = [c for c in cols if c in proj_df.columns]

                if proj_df.empty:
                    st.info("No props match filters.")
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
    # 📊 PLAYER CONTEXT HUB — Combines Minutes/Usage + Depth Chart + WOWY
    # ------------------------------------------------------
    with tab4:

        st.subheader("📊 Player Context Hub")

        # Three subtabs that replace Tab 6, Tab 9, Tab 10
        subA, subB, subC = st.tabs(
            [
                "⏱️ Minutes & Usage",
                "📋 Depth Chart & Injuries",
                "🔀 WOWY Analyzer",
            ]
        )

        # ======================================================
        # SUBTAB A — MINUTES & USAGE (Old Tab 6)
        # ======================================================
        with subA:

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


        # ======================================================
        # SUBTAB B — DEPTH CHART & INJURY REPORT (Old Tab 9)
        # ======================================================
        with subB:

            st.subheader("Depth Chart & Injury Report")

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

                            # Lookup injury status
                            inj_status = None
                            injury_html = ""

                            if not team_injuries.empty:
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


        # ======================================================
        # SUBTAB C — WOWY ANALYZER (Old Tab 10)
        # ======================================================
        with subC:

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


    # ======================================================
    # 📋 SAVED BETS TAB — PIKKIT OPTIMIZED EXPORT
    # ======================================================

    import pandas as pd
    import streamlit.components.v1 as components

    with tab8:

        st.header("Saved Bets")

        # --------------------------------------------------
        # Load saved bets
        # --------------------------------------------------
        slip = st.session_state.get("saved_bets", [])
        slip_df = pd.DataFrame(slip)

        if slip_df.empty:
            st.info("You haven't saved any bets yet.")
            st.stop()

        # --------------------------------------------------
        # Helper to normalize book structure
        # --------------------------------------------------
        def normalize_books(v):
            """Ensure books=[{'bookmaker':..., 'price':...}]"""
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return [{"bookmaker": k, "price": v[k]} for k in v]
            return []

        if "books" not in slip_df.columns:
            slip_df["books"] = [[] for _ in range(len(slip_df))]
        
        slip_df["books"] = slip_df["books"].apply(normalize_books)

        # --------------------------------------------------
        # Display saved bets
        # --------------------------------------------------
        st.markdown("### 🧾 Your Saved Bets")

        for idx, row in slip_df.iterrows():

            title = row.get("player") or row.get("team") or "Bet"
            st.markdown(f"## 🎯 {title}")

            is_prop = bool(row.get("bet_type"))
            market = row.get("market", "—")
            line = row.get("line", "—")

            # PROP
            if is_prop:
                st.markdown(
                    f"""
                    **Player Prop — {market}**  
                    **{row['bet_type'].upper()} {line}**
                    """
                )
            # GAME LINE
            else:
                st.markdown(
                    f"""
                    **Game Line — {market}**  
                    Team: **{row.get('team', '—')}**  
                    Line: **{line}**
                    """
                )

            # ODDS
            st.markdown("**Available Odds:**")
            books = normalize_books(row["books"])
            if books:
                for b in books:
                    st.markdown(f"- **{b['bookmaker']}**: {b['price']}")
            else:
                st.markdown("- No odds available")

            # REMOVE INDIVIDUAL
            if st.button("❌ Remove", key=f"remove_{idx}"):
                slip.pop(idx)
                st.session_state["saved_bets"] = slip
                st.rerun()

            st.markdown("---")

        # --------------------------------------------------
        # REMOVE ALL BETS
        # --------------------------------------------------
        if st.button("🗑️ Remove All Bets"):
            st.session_state["saved_bets"] = []
            st.success("All saved bets removed.")
            st.rerun()

        # ======================================================
        # 📲 EXPORT FOR PIKKIT — SELECTABLE TEXT BOX
        # ======================================================
        st.markdown("### 📲 Export for Pikkit")
        st.markdown("Copy the slip below and paste it into Pikkit using the ➕ button.")

        # --------------------------------------------------
        # BUILD PIKKIT-FORMATTED TEXT
        # --------------------------------------------------
        export_lines = []

        for _, row in slip_df.iterrows():

            is_prop = bool(row.get("bet_type"))
            player = row.get("player", "")
            team = row.get("team", "")
            market = row.get("market", "—")
            line = row.get("line", "—")

            # PROP FORMAT
            if is_prop:
                export_lines.append(f"{player} — {row['bet_type'].upper()} {line} ({market})")
            else:
                export_lines.append(f"{team} — {market} {line}")

            # BEST ODDS
            books = normalize_books(row["books"])
            if books:
                best = sorted(
                    books,
                    key=lambda x: (x["price"] >= 0, abs(x["price"]))
                )[0]
                export_lines.append(f"Best Odds: {best['price']} ({best['bookmaker']})")

            export_lines.append("")  # blank line between slips

        pikkit_text = "\n".join(export_lines).strip()

        # --------------------------------------------------
        # SELECTABLE EXPORT TEXT BOX (iPhone Safe)
        # --------------------------------------------------
        st.text_area(
            "Pikkit Import Text",
            pikkit_text,
            height=220,
            key="pikkit_textbox"
        )

        # --------------------------------------------------
        # SELECT ALL & COPY BUTTON (JS - works on iPhone)
        # --------------------------------------------------
        components.html(
            f"""
            <script>
                function copyPikkit() {{
                    const ta = parent.document.querySelector('textarea[id="pikkit_textbox"]');
                    if (!ta) return;
                    ta.focus();
                    ta.select();
                    document.execCommand('copy');
                }}
            </script>

            <button onclick="copyPikkit()"
                style="
                    background-color:#4CAF50;
                    color:white;
                    padding:10px 18px;
                    font-size:16px;
                    border:none;
                    border-radius:8px;
                    cursor:pointer;
                    margin-top:5px;
                ">
                📋 Select All & Copy
            </button>
            """,
            height=70,
        )

        # --------------------------------------------------
        # OPEN PIKKIT BUTTON (Universal Link)
        # --------------------------------------------------
        st.link_button("📲 Open Pikkit", "https://quickpick.pikkit.com")


# ------------------------------------------------------
# NCAA MEN'S / WOMEN'S — REAL MODULE
# ------------------------------------------------------
elif sport in ["NCAA Men's", "NCAA Women's"]:

    tabN1, tabN2, tabN3, tabN4, tabN5 = st.tabs(
        [
            "🏀 Game Overview",
            "💰 Moneyline",
            "📏 Spread",
            "🔢 Totals",
            "📋 Saved Bets",
        ]
    )

    # Load data
    df = ncaab_game_analytics_df.copy()

    if df.empty:
        st.info("No NCAA game analytics loaded. Make sure the loader is running.")
        st.stop()

    # -------------------------------
    # TAB 1 — OVERVIEW CARDS
    # -------------------------------
    with tabN1:
        st.subheader(f"{sport} — Game Overview")

        for idx, row in df.iterrows():
            render_ncaab_overview_card(row)

    # -------------------------------
    # TAB 2 — MONEYLINE
    # -------------------------------
    with tabN2:
        st.subheader(f"{sport} — Moneyline Analysis")

        ml_df = df.copy()

        # Ranking metric
        if "proj_margin" in ml_df.columns:
            ml_df["ml_strength"] = ml_df["proj_margin"]
        elif "predicted_margin" in ml_df.columns:
            ml_df["ml_strength"] = ml_df["predicted_margin"]

        ml_df = ml_df.sort_values("ml_strength", ascending=False)

        st.dataframe(
            ml_df[
                [
                    "game",
                    "start_time",
                    "home_team",
                    "away_team",
                    "home_ml",
                    "away_ml",
                    "proj_home_points",
                    "proj_away_points",
                    "proj_margin",
                ]
            ],
            use_container_width=True,
        )

    # -------------------------------
    # TAB 3 — SPREAD ANALYSIS
    # -------------------------------
    with tabN3:
        st.subheader(f"{sport} — Spread Analysis")

        st.dataframe(
            df.sort_values("spread_edge", ascending=False)[
                [
                    "game",
                    "start_time",
                    "home_team",
                    "away_team",
                    "home_spread",
                    "away_spread",
                    "proj_margin",
                    "spread_edge",
                ]
            ],
            use_container_width=True,
        )

    # -------------------------------
    # TAB 4 — TOTALS ANALYSIS
    # -------------------------------
    with tabN4:
        st.subheader(f"{sport} — Total Points Analysis")

        st.dataframe(
            df.sort_values("total_edge", ascending=False)[
                [
                    "game",
                    "start_time",
                    "home_team",
                    "away_team",
                    "total_line",
                    "proj_total_points",
                    "pace_proxy",
                    "total_edge",
                ]
            ],
            use_container_width=True,
        )

    # -------------------------------
    # TAB 5 — SAVED BETS
    # -------------------------------
    with tabN5:
        render_saved_bets_tab()



# ------------------------------------------------------
# LAST UPDATED FOOTER
# ------------------------------------------------------
now = datetime.now(EST)
st.sidebar.markdown(f"**Last Updated:** {now.strftime('%b %d, %I:%M %p')} ET")
