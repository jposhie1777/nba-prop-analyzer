import os
os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"] = "none"

# ------------------------------------------------------
# NBA Prop Analyzer - Merged Production + Dev UI
# ------------------------------------------------------
import os
import json
from datetime import datetime
from urllib.parse import urlencode

from dotenv import load_dotenv
load_dotenv()

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

from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from google.cloud import bigquery
from google.oauth2 import service_account



#def mem_diff(label: str):
    #gc.collect()
    #print(f"\n===== MEMORY DIFF: {label} =====")
    #st.session_state.mem_tracker.print_diff()

# ------------------------------------------------------
# STREAMLIT CONFIG (MUST BE FIRST STREAMLIT COMMAND)
# ------------------------------------------------------
st.set_page_config(
    page_title="NBA Prop Analyzer (DEV)",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ------------------------------------------------------
# SESSION INITIALIZATION (SAFE — NO STOP)
# ------------------------------------------------------
if "session_initialized" not in st.session_state:
    st.session_state["session_initialized"] = True

# ------------------------------------------------------
# SAFE QUERY PARAM NAVIGATION (NO RERUN)
# ------------------------------------------------------
if "pending_tab" in st.session_state:
    st.query_params["tab"] = st.session_state.pop("pending_tab")

# ✅ OK to call Streamlit stuff AFTER this point
st.sidebar.markdown("🧪 DEV_APP.PY RUNNING")

IS_DEV = True

import psutil

def get_mem_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / 1e6

# ======================================================
# DEV ACCESS CONTROL (EARLY)
# ======================================================
DEV_EMAILS = {
    "benvrana@bottleking.com",
    "jposhie1777@gmail.com",
}

def get_user_email():
    # 1️⃣ Session state (DEV override)
    user = st.session_state.get("user")
    if user and user.get("email"):
        return user["email"]

    # 2️⃣ Streamlit hosted auth (prod)
    try:
        email = st.experimental_user.email
        if email:
            return email
    except Exception:
        pass

    # 3️⃣ DEV fallback
    if IS_DEV:
        return "benvrana@bottleking.com"

    return None


def is_dev_user():
    return get_user_email() in DEV_EMAILS

# ======================================================
# SAFE TAB ROUTER (DEV + MAIN)
# ======================================================
def get_active_tab():
    tab = st.query_params.get("tab")
    if isinstance(tab, list):
        tab = tab[0]
    return tab or "main"

# ------------------------------------------------------
# DEV-SAFE BIGQUERY and GAS CONSTANTS
# ------------------------------------------------------
DEV_BQ_DATASET = os.getenv("BIGQUERY_DATASET", "nba_prop_analyzer")

DEV_SP_TABLES = {
    "Game Analytics": "game_analytics",
    "Game Report": "game_report",
    "Historical Player Stats (Trends)": "historical_player_stats",
    "Today's Props – Enriched": "todays_props_enriched",
    "Today's Props – Hit Rates": "todays_props_hit_rates",
}

# ======================================================
# DEV: BigQuery Client (Explicit Credentials)
# ======================================================
@st.cache_resource
def get_dev_bq_client():
    creds_dict = json.loads(os.getenv("GCP_SERVICE_ACCOUNT", ""))
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery",
        ],
    )
    project_id = os.getenv("PROJECT_ID")

    return bigquery.Client(credentials=creds, project=project_id)

@st.cache_data(ttl=1800, show_spinner=False)
def load_bq_df(sql: str) -> pd.DataFrame:
    df = bq_client.query(sql).to_dataframe()
    df.flags.writeable = False
    return df

# ======================================================
# DEV: Google Apps Script Trigger
# ======================================================
def trigger_apps_script(task: str):
    try:
        url = os.getenv("APPS_SCRIPT_URL")
        token = os.getenv("APPS_SCRIPT_DEV_TOKEN")

        if not url:
            raise RuntimeError("APPS_SCRIPT_URL is not set")
        if not token:
            raise RuntimeError("APPS_SCRIPT_DEV_TOKEN is not set")

        resp = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
            },
            params={          # 👈 ADD THIS
                "token": token
            },
            json={"task": task},
            timeout=60,
        )


        data = resp.json()

        if not data.get("success"):
            raise RuntimeError(data.get("message"))

        st.success(f"✅ {data.get('message')}")

    except Exception as e:
        st.error("❌ Apps Script trigger failed")
        st.code(str(e))

@st.cache_data(ttl=3600, show_spinner=False)
def get_table_schema(dataset: str, table: str) -> pd.DataFrame:
    query = f"""
    SELECT column_name, data_type
    FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table}'
    ORDER BY ordinal_position
    """
    df = load_bq_df(query)
    df.flags.writeable = False
    return df

# ======================================================
# DEV: BigQuery Stored Procedure Trigger (SAFE)
# ======================================================
def trigger_bq_procedure(proc_name: str):
    try:
        client = get_dev_bq_client()
        sql = f"CALL `{DEV_BQ_DATASET}.{proc_name}`()"
        job = client.query(sql)
        job.result()  # wait, but pull no data
        st.success(f"✅ {proc_name} completed")
    except Exception as e:
        st.error(f"❌ {proc_name} failed")
        st.code(str(e))

from googleapiclient.discovery import build
from google.oauth2 import service_account


def read_sheet_values(sheet_id: str, range_name: str) -> list[list[str]]:
    """
    Read values from a Google Sheet range.
    Read-only, no caching, no memory retention.
    """
    creds_dict = json.loads(os.getenv("GCP_SERVICE_ACCOUNT", ""))
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )

    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range=range_name)
        .execute()
    )

    return resp.get("values", [])


# ======================================================
# DEV PAGE OVERRIDE (CRASH-SAFE)
# ======================================================
def render_dev_page():
    st.title("⚙️ DEV CONTROL PANEL")
    
    if st.button("⬅ Back to Main App", use_container_width=False):
        st.session_state["pending_tab"] = "main"
    
    st.caption("Always available • restricted access")

    st.markdown(f"**Email:** `{get_user_email()}`")

    st.divider()

    st.subheader("🧪 BigQuery – Manual Stored Procedure Triggers")

    BQ_PROCS = [
        ("Game Analytics", "sp_game_analytics"),
        ("Game Report", "sp_game_report"),
        ("Historical Player Stats (Trends)", "sp_historical_player_stats_for_trends"),
        ("Today's Props – Enriched", "sp_todays_props_enriched"),
        ("Today's Props – Hit Rates", "sp_todays_props_with_hit_rates"),
    ]

    for label, proc in BQ_PROCS:
        col1, col2 = st.columns([3, 1])

        with col1:
            st.markdown(f"**{label}**")
            st.caption(f"`{DEV_BQ_DATASET}.{proc}`")

        with col2:
            if st.button(
                "▶ Run",
                key=f"run_{proc}",
                use_container_width=True
            ):
                with st.spinner(f"Running {proc}…"):
                    trigger_bq_procedure(proc)


    st.divider()

    st.subheader("Cloud Run")
    if st.button("▶ Trigger ESPN Lineups"):
        trigger_cloud_run("espn-nba-lineups")

    st.divider()

    st.subheader("📄 Google Apps Script")

    APPS_TASKS = [
        ("NBA Alternate Props", "NBA_ALT_PROPS"),
        ("NBA Game Odds", "NBA_GAME_ODDS"),
        ("NCAAB Game Odds", "NCAAB_GAME_ODDS"),
        ("Run ALL (Daily Runner)", "ALL"),
    ]

    for label, task in APPS_TASKS:
        col1, col2 = st.columns([3, 1])

        with col1:
            st.markdown(f"**{label}**")

        with col2:
            if st.button(
                "▶ Run",
                key=f"apps_{task}",
                use_container_width=True
            ):
                with st.spinner(f"Running {label}…"):
                    trigger_apps_script(task)

    st.divider()
    st.subheader("📊 Google Sheet Sanity Checks")

    SHEET_ID = "1p_rmmiUgU18afioJJ3jCHh9XeX7V4gyHd_E0M3A8M3g"

    st.markdown("## 🧪 Stored Procedure Outputs – Schema Preview")

    for label, table in DEV_SP_TABLES.items():
        st.subheader(label)
    
        with st.expander("📋 View Columns"):
            try:
                schema_df = get_table_schema("nba_prop_analyzer", table)
    
                if schema_df.empty:
                    st.warning("No columns found (table may not exist yet).")
                else:
                    st.dataframe(
                        schema_df,
                        use_container_width=True,
                        hide_index=True
                    )
    
            except Exception as e:
                st.error(f"Failed to load schema: {e}")

    # --------------------------------------------------
    # 1) Odds tab checks
    # --------------------------------------------------
    try:
        odds_rows = read_sheet_values(SHEET_ID, "Odds!A:I")

        has_odds_data = len(odds_rows) > 1

        labels = []
        if has_odds_data:
            labels = [
                (r[8] or "").strip().lower()
                for r in odds_rows[1:]
                if len(r) >= 9
            ]

        has_over = any("over" in l for l in labels)
        has_under = any("under" in l for l in labels)

        st.markdown("**Odds Tab**")

        if has_odds_data:
            st.success("✅ Rows exist after header")
        else:
            st.error("❌ No rows found after header")

        if has_over and has_under:
            st.success("✅ Both Over and Under found in `label` column")
        elif has_over:
            st.warning("⚠️ Only Over found in `label` column")
        elif has_under:
            st.warning("⚠️ Only Under found in `label` column")
        else:
            st.error("❌ No Over / Under values found in `label` column")

    except Exception as e:
        st.error("❌ Failed to read Odds tab")
        st.code(str(e))


    # --------------------------------------------------
    # 2) Game Odds Sheet checks
    # --------------------------------------------------
    try:
        game_odds_rows = read_sheet_values(SHEET_ID, "Game Odds Sheet!A:A")

        has_game_odds_data = len(game_odds_rows) > 1

        st.markdown("**Game Odds Sheet**")

        if has_game_odds_data:
            st.success("✅ Rows exist after header")
        else:
            st.error("❌ No rows found after header")

    except Exception as e:
        st.error("❌ Failed to read Game Odds Sheet")
        st.code(str(e))


        st.success("DEV page loaded successfully.")



# ======================================================
# EARLY EXIT — NOTHING BELOW THIS CAN BLOCK DEV PAGE
# ======================================================
active_tab = get_active_tab()

# ---------------- DEV TAB (CRASH SAFE) ----------------
if active_tab == "dev":
    if not is_dev_user():
        st.error("⛔ Access denied")
        st.stop()

    render_dev_page()
    st.stop()


# ------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "")

DATASET = os.getenv("BIGQUERY_DATASET", "nba_prop_analyzer")
PROPS_TABLE = "todays_props_enriched"
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

if missing_env and not IS_DEV:
    st.error(
        "❌ Missing required environment variables:\n\n"
        + "\n".join(f"- {m}" for m in missing_env)
    )
    st.stop()

if missing_env and IS_DEV:
    st.warning(
        "⚠️ DEV MODE: Missing env vars ignored:\n\n"
        + "\n".join(f"- {m}" for m in missing_env)
    )


# -------------------------------
# Saved Bets (constant-memory)
# -------------------------------
MAX_SAVED_BETS = 150  # keep this small + stable

def _bet_key(player, market, line, bet_type):
    # minimal stable key — avoids duplicates + memory bloat
    return f"{player}|{market}|{line}|{bet_type}".lower().strip()

if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

if "saved_bets_keys" not in st.session_state:
    st.session_state.saved_bets_keys = set()


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
#DELTA_SQL = f"""
#SELECT *
#FROM {PROJECT_ID}.nba_prop_analyzer.player_wowy_deltas
#"""

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
#NCAAB_GAME_ANALYTICS_SQL = f"""
#SELECT *
#FROM `{PROJECT_ID}.ncaa_data.ncaab_game_analytics`
#ORDER BY start_time
#"""


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
import os
import psycopg2

def get_db_conn():
    """
    Create a new Postgres connection.
    Short-lived, DB-only, safe for Streamlit reruns.
    """
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        sslmode="require",
    )

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


def render_landing_nba_games():
    st.subheader("🏀 NBA Games Today")

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

    try:
        df = load_bq_df(sql)
    except Exception as e:
        st.info("NBA games for today will appear here.")
        st.caption(str(e))
        return

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
            unsafe_allow_html=True,
        )

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
# AUTH FLOW
# ------------------------------------------------------
if IS_DEV:
    st.session_state["user"] = {
        "auth0_sub": "dev-user",
        "email": "benvrana@bottleking.com",
    }
    st.session_state["user_id"] = -1
else:
    ensure_logged_in()

# ------------------------------------------------------
# NOT LOGGED IN → SHOW LANDING SCREEN (PROD ONLY)
# ------------------------------------------------------
if not IS_DEV and "user" not in st.session_state:
    st.title("Pulse Sports Analytics")
    st.caption("Daily games, props, trends, and analytics")

    login_url = get_auth0_authorize_url()
    st.markdown(
        f"""
        <div style="margin: 14px 0;">
            <a href="{login_url}">🔐 Log in with Auth0</a>
        </div>
        """,
        unsafe_allow_html=True,
    )

    render_landing_nba_games()
    st.stop()

# ------------------------------------------------------
# REQUIRE LOGIN
# ------------------------------------------------------
user = st.session_state["user"]
user_id = st.session_state["user_id"]
st.sidebar.markdown(f"**User:** {user.get('email') or 'Logged in'}")


# ------------------------------------------------------
# DEV TOOLS UI TAB (VISUAL ONLY)
# ------------------------------------------------------
if IS_DEV and is_dev_user():
    st.sidebar.divider()
    st.sidebar.markdown("### ⚙️ Dev Tools")

    if st.sidebar.button("Open DEV Tools"):
        st.query_params["tab"] = "dev"
        st.rerun()


# ------------------------------------------------------
# LOCKED THEME (STATIC) AND GLOBAL STYLES
# ------------------------------------------------------
THEME_BG = "#020617"
THEME_ACCENT = "#0ea5e9"
THEME_ACCENT_SOFT = "#0369a1"


@st.cache_resource
def load_static_ui():
    st.markdown(
        f"""
        <style>
        /* ---------- GLOBAL THEME ---------- */
        html, body, [class*="css"] {{
            font-family: "Inter", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }}

        body {{
            background: radial-gradient(circle at top, {THEME_BG} 0, #000 55%) !important;
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

        /* ---------- BUTTONS ---------- */
        .stButton > button {{
            border-radius: 999px !important;
            padding: 0.35rem 0.95rem !important;
            font-weight: 600 !important;
            border: 1px solid rgba(148,163,184,0.4) !important;
            background: radial-gradient(
                circle at 0 0,
                {THEME_ACCENT},
                {THEME_ACCENT_SOFT} 50%,
                #020617 100%
            );
            color: #f9fafb !important;
            box-shadow: 0 12px 30px rgba(8,47,73,0.9);
        }}

        .stButton > button:hover {{
            transform: translateY(-1px) scale(1.01);
            box-shadow: 0 16px 40px rgba(8,47,73,1);
        }}

        /* ---------- CARD TAP ---------- */
        .card-tap-btn .stButton > button {{
            all: unset !important;
            display: block !important;
            width: 100% !important;
            height: 50px !important;
            cursor: pointer !important;
            background: transparent !important;
        }}

        /* ---------- PROP CARDS ---------- */
        .prop-card-wrapper {{
            position: relative;
            z-index: 5;
            border-radius: 14px;
        }}

        .prop-card-wrapper summary {{
            cursor: pointer;
            list-style: none;
        }}

        .prop-card-wrapper summary::-webkit-details-marker {{
            display: none;
        }}

        .prop-card-wrapper summary * {{
            pointer-events: none;
        }}

        .prop-card-wrapper .card-expanded {{
            margin-top: 8px;
            pointer-events: auto;
        }}

        .expand-hint {{
            text-align: center;
            font-size: 0.7rem;
            opacity: 0.65;
            margin-top: 6px;
        }}

        /* ---------- EXPANDED METRICS ---------- */
        .expanded-wrap {{
            margin-top: 8px;
            padding: 10px;
            border-radius: 12px;
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.12);
        }}

        .expanded-row {{
            display: flex;
            justify-content: space-between;
            gap: 8px;
            margin-bottom: 6px;
        }}

        .metric {{
            flex: 1;
            text-align: center;
            font-size: 0.72rem;
        }}

        .metric span {{
            display: block;
            color: #9ca3af;
        }}

        .metric strong {{
            font-size: 0.85rem;
            font-weight: 700;
            color: #ffffff;
        }}

        /* ---------- AG GRID MOBILE ---------- */
        .ag-theme-balham .ag-center-cols-container {{
            min-width: 1100px !important;
        }}

        .ag-theme-balham .ag-body-viewport,
        .ag-theme-balham .ag-root {{
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    components.html(
        """
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
        </style>
        """,
        height=0,
    )

# ------------------------------------------------------
# LOAD STATIC UI (CACHED — RUNS ONCE PER SESSION)
# ------------------------------------------------------
load_static_ui()


# ------------------------------------------------------
# SPARKLINE WINDOW CONFIG
# ------------------------------------------------------
SPARK_WINDOWS = {
    "L5": {
        "vals_col": "pts_last5_list",
        "avg_col": "pts_last5_avg",
        "width": 120,
    },
    "L20": {
        "vals_col": "pts_last20_list",
        "avg_col": "pts_last20_avg",
        "width": 160,
    },
}


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

<div id="scrollTopBtn" onclick="scrollToTop()">▲ Top</div>
""", unsafe_allow_html=True)

# --------------------------------
# SPORT SELECTOR (TOP, ABOVE HEADER)
# --------------------------------
col1, col2 = st.columns([4, 1])

with col1:
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

def _safe_float(x):
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None

def _fmt1(x):
    return f"{float(x):.1f}" if x is not None and not pd.isna(x) else f"-"

def _fmt_signed1(x):
    if x is None or pd.isna(x):
        return f"-"
    return f"{float(x):+.1f}"

def _norm_name(s: str) -> str:
    return (
        str(s or "")
        .lower()
        .replace(".", "")
        .replace("'", "")
        .replace("-", " ")
        .replace(" jr", "")
        .replace(" sr", "")
        .strip()
    )

def _get_stat_list_for_market(row, n: int):
    stat = detect_stat(row.get("market", ""))
    if not stat:
        return []
    col = f"{stat}_last{n}_list"
    v = row.get(col)
    if v is None:
        return []
    try:
        return list(v)
    except Exception:
        return []

def _avg_last(values: list):
    vals = []
    for x in values:
        if isinstance(x, (int, float)) and not pd.isna(x):
            vals.append(float(x))
    if not vals:
        return None
    return float(sum(vals) / len(vals))

def _team_injuries_for_team(team_abbrev: str, exclude_player: str):
    if injury_df is None or injury_df.empty:
        return []

    t = str(team_abbrev or "").strip().upper()
    ex_norm = _norm_name(exclude_player)

    df = injury_df.copy()
    df = df[df["team_abbrev"].astype(str).str.upper() == t]

    out = []
    for _, r in df.iterrows():
        nm = str(r.get("full_name", "") or "")
        if _norm_name(nm) == ex_norm:
            continue

        stt = str(r.get("status", "") or "").strip()
        abbr = str(r.get("status_type_abbr", "") or "").strip()
        label = abbr if abbr else stt

        if label:
            out.append({"name": nm, "status": label})

    # de-dupe by name keeping first (newest snapshot_ts already sorted DESC in SQL)
    seen = set()
    dedup = []
    for x in out:
        key = _norm_name(x["name"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(x)

    return dedup

def _market_to_wowy_col(market: str):
    m = str(market or "").lower()
    if "points_rebounds_assists" in m or "pra" in m:
        return "pra_delta"
    if "points_rebounds" in m or " pr" in m:
        return "pts_reb_delta"
    if "points_assists" in m or " pa" in m:
        return "pa_delta"
    if "rebounds_assists" in m or " ra" in m:
        return "ra_delta"
    if "assist" in m or "ast" in m:
        return "ast_delta"
    if "rebound" in m or "reb" in m:
        return "reb_delta"
    if "point" in m or "pts" in m:
        return "pts_delta"
    return None

def _injury_wowy_impact_for_name(wowy_list: list[dict], injured_name: str, market: str):
    col = _market_to_wowy_col(market)
    if not col:
        return None

    target = _norm_name(injured_name)
    for w in (wowy_list or []):
        b = str(w.get("breakdown", "") or "")
        # your breakdown often has teammate then arrow
        left = b.split("→")[0].strip()
        if target and target in _norm_name(left):
            v = w.get(col)
            v2 = _safe_float(v)
            if v2 is not None:
                return float(v2)
    return None

def _confidence_index(
    hit_rate,
    implied_prob,
    delta_vs_line,
    minutes_delta=None,        # 👈 OPTIONAL
    inj_impact_sum=None,
):
    # simple + stable: 0..100
    hr = float(hit_rate or 0.0)
    ip = float(implied_prob or 0.0)
    edge = hr - ip

    base = 50.0
    base += max(-20.0, min(25.0, edge * 120.0))

    if delta_vs_line is not None:
        base += max(-10.0, min(10.0, float(delta_vs_line) * 2.5))

    if minutes_delta is not None:
        # est_minutes - l5_min_avg : negative hurts
        base += max(-18.0, min(10.0, float(minutes_delta) * 2.0))

    if inj_impact_sum is not None:
        base += max(-10.0, min(14.0, float(inj_impact_sum) * 3.0))

    return int(round(max(0.0, min(100.0, base))))

def _pct(v):
    return f"{v*100:.0f}%" if v is not None else "—"

def _pm(v):
    return f"{v:+.1f}" if v is not None else "—"

from functools import lru_cache

# ======================================================
# WOWY market → delta column mapping
# ======================================================
WOWY_STAT_MAP = {
    "PTS": "pts_delta",
    "REB": "reb_delta",
    "AST": "ast_delta",
    "STL": "stl_delta",
    "BLK": "blk_delta",

    "PRA": "pra_delta",
    "PR": "pts_reb_delta",
    "PA": "pts_ast_delta",
    "RA": "reb_ast_delta",
}

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

SPORTSBOOK_LOGOS = {
    "DraftKings": str(LOGO_DIR / "Draftkingssmall.png"),
    "FanDuel": str(LOGO_DIR / "Fanduelsmall.png"),
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

# ------------------------------------------------------
# CACHED TEAM LOGOS (BASE64) — RUNS ONCE PER SESSION
# ------------------------------------------------------
@st.cache_resource
def get_team_logos_base64():
    return {
        code: logo_to_base64_url(url)
        for code, url in TEAM_LOGOS.items()
    }

TEAM_LOGOS_BASE64 = get_team_logos_base64()

@st.cache_resource
def get_sportsbook_logos_base64():
    return {
        name: logo_to_base64_local(path)
        for name, path in SPORTSBOOK_LOGOS.items()
    }

SPORTSBOOK_LOGOS_BASE64 = get_sportsbook_logos_base64()


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

def format_wowy_html(wowy_raw, stat_prefix):
    if not isinstance(wowy_raw, str) or not wowy_raw.strip():
        return "<span class='wowy-empty'>No injury impact</span>"

    blocks = [b.strip() for b in wowy_raw.split(";") if b.strip()]
    lines = []

    for block in blocks:
        if "→" not in block:
            continue

        name_part, stats_part = block.split("→", 1)

        stats = {}
        for s in stats_part.split(","):
            if "=" in s:
                k, v = s.strip().split("=", 1)
                try:
                    stats[k] = float(v)
                except ValueError:
                    pass

        vals = extract_wowy_value(stats, stat_prefix)

        if isinstance(vals, tuple):
            vals = [v for v in vals if v is not None]
            if not vals:
                continue
            total = sum(vals)
        else:
            if vals is None:
                continue
            total = vals

        lines.append(
            f"<div class='wowy-line'>"
            f"<strong>{name_part.strip()}</strong>: {total:+.2f}"
            f"</div>"
        )

    if not lines:
        return "<span class='wowy-empty'>No relevant injury impact</span>"

    return "<div class='wowy-container'>" + "".join(lines) + "</div>"

def extract_wowy_value(stats_dict, stat_prefix):
    stat_prefix = stat_prefix.upper()

    if stat_prefix == "PA":
        return stats_dict.get("PTS"), stats_dict.get("AST")

    if stat_prefix == "PR":
        return stats_dict.get("PTS"), stats_dict.get("REB")

    if stat_prefix == "RA":
        return stats_dict.get("REB"), stats_dict.get("AST")

    # Single stat
    return stats_dict.get(stat_prefix)

@st.cache_data(show_spinner=False)
def build_prop_cards(card_df, hit_rate_col):
    PROP_KEY_COLS = [
        "player",
        "player_team",
        "opponent_team",
        "market",
        "line",
        "bet_type",
    ]

    work = card_df.copy()

    work["book_norm"] = work["bookmaker"].apply(normalize_bookmaker)
    work["price_int"] = pd.to_numeric(work["price"], errors="coerce")

    work = work.dropna(subset=["price_int"])
    work["price_int"] = work["price_int"].astype(int)

    base = (
        work.sort_values(by=[hit_rate_col], ascending=False)
            .drop_duplicates(PROP_KEY_COLS, keep="first")
            .copy()
    )

    dedup_books = work.drop_duplicates(PROP_KEY_COLS + ["book_norm", "price_int"])

    rows = []
    for key, sub in dedup_books.groupby(PROP_KEY_COLS, dropna=False, sort=False):
        book_prices = [
            {"book": b, "price": int(p)}
            for b, p in zip(sub["book_norm"], sub["price_int"])
            if b is not None and p is not None
        ]
        rows.append((*key, book_prices))

    books_df = pd.DataFrame(rows, columns=PROP_KEY_COLS + ["book_prices"])

    card_df = base.merge(books_df, on=PROP_KEY_COLS, how="left")
    card_df["book_prices"] = card_df["book_prices"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    return card_df

def save_bet_simple(player, market, line, price, bet_type):
    bets = st.session_state.saved_bets
    keys = st.session_state.saved_bets_keys

    k = _bet_key(player, market, line, bet_type)
    if k in keys:
        return False  # already saved, no growth

    bet = {
        "player": str(player or ""),
        "market": str(market or ""),
        "line": float(line) if line is not None else None,
        "price": int(price) if price is not None else None,
        "bet_type": str(bet_type or ""),
    }

    bets.append(bet)
    keys.add(k)

    # hard cap (evict oldest)
    if len(bets) > MAX_SAVED_BETS:
        old = bets.pop(0)
        oldk = _bet_key(old.get("player"), old.get("market"), old.get("line"), old.get("bet_type"))
        keys.discard(oldk)

    return True


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
@st.cache_data(ttl=1800, show_spinner=True)
def load_props() -> pd.DataFrame:
    # --------------------------------------------------
    # Load once from BigQuery (ONLY entry point)
    # --------------------------------------------------
    df = load_bq_df(PROPS_SQL)

    # --------------------------------------------------
    # Column normalization (safe)
    # --------------------------------------------------
    df.columns = df.columns.str.strip()

    # --------------------------------------------------
    # Datetime normalization
    # --------------------------------------------------
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    # --------------------------------------------------
    # String normalization
    # --------------------------------------------------
    for col in ("home_team", "visitor_team", "opponent_team"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    # --------------------------------------------------
    # Core numerics
    # --------------------------------------------------
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    for col in ("hit_rate_last5", "hit_rate_last10", "hit_rate_last20"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --------------------------------------------------
    # Matchup difficulty normalization
    # --------------------------------------------------
    if "matchup_difficulty_by_stat" in df.columns:
        df["matchup_difficulty_score"] = pd.to_numeric(
            df["matchup_difficulty_by_stat"], errors="coerce"
        )
    elif "matchup_difficulty_score" in df.columns:
        df["matchup_difficulty_score"] = pd.to_numeric(
            df["matchup_difficulty_score"], errors="coerce"
        )

    # --------------------------------------------------
    # EV / edge / projection / minutes usage numerics
    # --------------------------------------------------
    numeric_cols = (
        "ev_last5", "ev_last10", "ev_last20",
        "implied_prob", "edge_raw", "edge_pct",
        "proj_last10", "proj_std_last10", "proj_volatility_index",
        "proj_diff_vs_line",
        "est_minutes", "usage_bump_pct",
    )

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --------------------------------------------------
    # 🔒 Freeze dataframe to protect cache integrity
    # --------------------------------------------------
    df.flags.writeable = False

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




@st.cache_data(ttl=1800, show_spinner=True)
def load_historical_df() -> pd.DataFrame:
    df = load_bq_df(HISTORICAL_SQL).copy()

    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["opponent_team"] = df["opponent_team"].fillna("").astype(str)

    # Convert stringified list columns
    df = convert_list_columns(df)

    return df

@st.cache_data(ttl=1800, show_spinner=True)
def load_depth_charts() -> pd.DataFrame:
    df = load_bq_df(DEPTH_SQL).copy()
    df.columns = df.columns.str.strip()
    return df


@st.cache_data(ttl=1800, show_spinner=True)
def load_injury_report() -> pd.DataFrame:
    df = load_bq_df(INJURY_SQL).copy()

    df.columns = df.columns.str.strip()
    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"], errors="coerce")

    return df


@st.cache_data(ttl=1800, show_spinner=True)
def load_wowy_deltas() -> pd.DataFrame:
    df = load_bq_df(DELTA_SQL).copy()
    df.columns = df.columns.str.strip()

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

@st.cache_data(ttl=1800, show_spinner=True)
def load_game_analytics() -> pd.DataFrame:
    df = load_bq_df(GAME_ANALYTICS_SQL).copy()

    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    return df

# ------------------------------------------------------
# LOAD NCAA GAME ANALYTICS
# ------------------------------------------------------
@st.cache_data(ttl=1800, show_spinner=True)
def load_ncaab_game_analytics() -> pd.DataFrame:
    df = load_bq_df(NCAAB_GAME_ANALYTICS_SQL).copy()
    df.columns = df.columns.str.strip()

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")

    numeric_cols = [
        "home_ml", "away_ml",
        "home_spread", "away_spread",
        "total_line",
        "proj_home_points", "proj_away_points", "proj_total_points",
        "proj_margin",
        "spread_edge", "total_edge",
        "l5_scoring_diff", "l10_scoring_diff",
        "l5_margin_diff", "l10_margin_diff",
        "pace_proxy",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

@st.cache_data(ttl=1800, show_spinner=True)
def load_game_report() -> pd.DataFrame:
    df = load_bq_df(GAME_REPORT_SQL).copy()

    df.columns = df.columns.str.strip()
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    return df

@st.cache_data(ttl=300, show_spinner=True)
def load_game_odds() -> pd.DataFrame:
    df = load_bq_df(GAME_ODDS_SQL).copy()

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df

def get_stat_prefix(row):
    market = str(row.get("market", "")).lower()

    # Normalize separators
    market = market.replace("+", "_").replace("-", "_")

    # Triple combo
    if "pts_reb_ast" in market or "pra" in market:
        return "pra"

    # Double combos
    if "pts_ast" in market or "points_assists" in market or "pa" in market:
        return "pa"
    if "pts_reb" in market or "points_rebounds" in market or "pr" in market:
        return "pr"
    if "reb_ast" in market or "rebounds_assists" in market or "ra" in market:
        return "ra"

    # Singles
    if "assists" in market or market == "ast":
        return "ast"
    if "rebounds" in market or market == "reb":
        return "reb"
    if "points" in market or market == "pts":
        return "pts"

    return None


def get_rolling_avg(row, window: int):
    prefix = get_stat_prefix(row)
    if not prefix:
        return None
    return row.get(f"{prefix}_last{window}")


# ------------------------------------------------------
# LOAD BASE TABLES
# ------------------------------------------------------
props_df = load_props()
history_df = load_historical_df()
depth_df = load_depth_charts()
injury_df = load_injury_report()    # <-- MUST COME BEFORE FIX
wowy_df = load_wowy_deltas()

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

MARKET_TO_PREFIX = {
    "pts": "pts",
    "reb": "reb",
    "ast": "ast",
    "pts+reb": "pr",
    "pts+ast": "pa",
    "reb+ast": "ra",
    "pts+reb+ast": "pra",
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

def compute_confidence(
    *,
    hit_rate_l10: float | None,
    delta_vs_line: float | None,
    opp_rank: int | None,
) -> tuple[float | None, str | None]:

    if hit_rate_l10 is None or delta_vs_line is None:
        return None, None

    hit_score = max(0, min(1, hit_rate_l10))

    MAX_DELTA = 8.0
    delta_score = max(
        0,
        min(1, (delta_vs_line + MAX_DELTA) / (2 * MAX_DELTA))
    )

    if opp_rank:
        opp_score = (opp_rank - 1) / 29
    else:
        opp_score = 0.5

    score = (
        0.4 * hit_score +
        0.4 * delta_score +
        0.2 * opp_score
    ) * 100

    if score >= 75:
        level = "Strong"
    elif score >= 60:
        level = "Medium"
    else:
        level = "Light"

    return round(score, 1), level

#=================================================
# Sparkline Variables
#=================================================
def get_spark_series(row, max_points: int = 12):
    """
    Extract sparkline values + dates for a prop.
    Priority: L10 → L7 → L5
    Memory-safe:
      - hard cap on points
      - no unbounded list growth
      - no pandas Series expansion
    """

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

        if vals is None or dates is None:
            continue

        # ---- SAFE slicing BEFORE list coercion ----
        try:
            vals_slice = vals[-max_points:]
            dates_slice = dates[-max_points:]
        except Exception:
            continue

        clean_vals = []
        clean_dates = []

        for v, d in zip(vals_slice, dates_slice):
            if not isinstance(v, (int, float)):
                continue

            clean_vals.append(float(v))

            try:
                clean_dates.append(
                    pd.to_datetime(d).strftime("%m/%d")
                )
            except Exception:
                clean_dates.append("")

        if not clean_vals:
            continue

        return clean_vals, clean_dates

    return [], []

def build_bar_sparkline_svg_with_lines(
    values: list,
    *,
    line_value: float | None = None,
    avg_value: float | None = None,
    width: int = 120,
    height: int = 36,
) -> str:
    """
    Bar sparkline with:
    - Green bars above line
    - Red bars below line
    - Dashed prop line
    - Optional avg line
    Works for L5, L10, L20, etc.
    """

    if not values:
        return ""

    # -----------------------------
    # Clean + coerce values
    # -----------------------------
    vals = [v for v in values if v is not None and not pd.isna(v)]
    if not vals:
        return ""

    vmin = min(vals)
    vmax = max(vals)

    if vmin == vmax:
        vmin -= 1
        vmax += 1

    pad_top = 3
    pad_bottom = 3

    def y(v):
        return (
            height - pad_bottom
            - ((v - vmin) / (vmax - vmin)) * (height - pad_top - pad_bottom)
        )

    # -----------------------------
    # Horizontal lines
    # -----------------------------
    overlays = ""

    if line_value is not None and vmin <= line_value <= vmax:
        y_line = y(line_value)
        overlays += (
            f"<line x1='0' y1='{y_line:.1f}' "
            f"x2='{width}' y2='{y_line:.1f}' "
            f"stroke='#ef4444' stroke-width='1' "
            f"stroke-dasharray='4,3' />"
        )

    if avg_value is not None and vmin <= avg_value <= vmax:
        y_avg = y(avg_value)
        overlays += (
            f"<line x1='0' y1='{y_avg:.1f}' "
            f"x2='{width}' y2='{y_avg:.1f}' "
            f"stroke='#38bdf8' stroke-width='1' />"
        )

    # -----------------------------
    # Bars
    # -----------------------------
    n = len(vals)
    gap = 1.5
    bar_width = (width - (n - 1) * gap) / n

    bars = []

    for i, v in enumerate(vals):
        bar_height = max(1, height - y(v) - pad_bottom)
        x = i * (bar_width + gap)
        y_pos = height - bar_height

        if line_value is not None and v < line_value:
            color = "#ef4444"  # red
        else:
            color = "#22c55e"  # green

        bars.append(
            f"<rect x='{x:.1f}' y='{y_pos:.1f}' "
            f"width='{bar_width:.1f}' height='{bar_height:.1f}' "
            f"rx='1.5' fill='{color}' />"
        )

    # -----------------------------
    # Final SVG
    # -----------------------------
    return (
        f"<svg width='{width}' height='{height}' "
        f"viewBox='0 0 {width} {height}' "
        f"preserveAspectRatio='none'>"
        f"{overlays}"
        f"{''.join(bars)}"
        f"</svg>"
    )
    
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

@st.cache_data(show_spinner=False)
def precompute_sparklines(df):
    out = {}

    for i, row in df.iterrows():
        key = (
            row.get("player"),
            row.get("market"),
            row.get("line"),
            row.get("game_id"),
        )

        vals, dates = get_spark_series(row)

        if not vals:
            out[key] = ""
            continue

        out[key] = build_sparkline_bars_hitmiss(
            vals,
            dates,
            float(row.get("line") or 0),
        )

    return out


def market_to_prefix(market: str | None) -> str | None:
    if not market:
        return None

    m = market.lower().replace(" ", "").replace("_", "").replace("-", "")

    # Singles
    if m in ("pts", "points"):
        return "pts"
    if m in ("reb", "rebs", "rebounds"):
        return "reb"
    if m in ("ast", "asts", "assists"):
        return "ast"
    if m in ("stl", "stls", "steals"):
        return "stl"
    if m in ("blk", "blks", "blocks"):
        return "blk"

    # Combos
    if m in ("ptsreb", "pointsrebounds", "pr"):
        return "pr"
    if m in ("ptsast", "pointsassists", "pa"):
        return "pa"
    if m in ("rebast", "reboundsassists", "ra"):
        return "ra"
    if m in ("ptsrebast", "pointsreboundsassists", "pra"):
        return "pra"

    return None

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
    if df.empty:
        st.info(f"No props match your filters.")
        return

    # ------------------------------------------------------
    # WOWY merge (already safe / cached)
    # ------------------------------------------------------
    card_df = attach_wowy_deltas(df, wowy_df)
    if card_df is None:
        st.error("attach_wowy_deltas returned None")
        st.stop()

    # ------------------------------------------------------
    # Restrict sportsbooks
    # ------------------------------------------------------
    card_df = card_df[
        card_df["bookmaker"].isin(
            ["DraftKings", "FanDuel", "draftkings", "fanduel"]
        )
    ]

    # ------------------------------------------------------
    # Row filter
    # ------------------------------------------------------
    def card_good(row: pd.Series) -> bool:
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
        st.info("No props match your filters (after logic).")
        return

    # ------------------------------------------------------
    # GROUP INTO UNIQUE PROPS (MULTI-BOOK)  ✅ NO groupby.apply
    # ------------------------------------------------------
    card_df = card_df[card_df.apply(card_good, axis=1)]

    if card_df.empty:
        st.info("No props match your filters (after logic).")
        return
    
    # ✅ CACHED HEAVY BUILD
    card_df = build_prop_cards(card_df, hit_rate_col)

    # ------------------------------------------------------
    # Sort
    # ------------------------------------------------------
    card_df = (
        card_df
        .sort_values(by=[hit_rate_col], ascending=False)
        .reset_index(drop=True)
    )

    # ------------------------------------------------------
    # Pagination
    # ------------------------------------------------------
    page_size = 30
    total_cards = len(card_df)
    total_pages = max(1, (total_cards + page_size - 1) // page_size)

    page = st.number_input(
        "Page",
        min_value=1,
        max_value=total_pages,
        value=1,
        step=1,
        key=f"{page_key}_page",
    )

    page_df = card_df.iloc[
        (page - 1) * page_size : page * page_size
    ]

    # ------------------------------------------------------
    # Precompute sparklines ONCE (CRITICAL for memory)
    # ------------------------------------------------------
    sparkline_map = precompute_sparklines(page_df)


    # ------------------------------------------------------
    # Scroll wrapper
    # ------------------------------------------------------
    st.markdown(
        f"<div style='max-height:1100px; overflow-y:auto; padding-right:12px;'>",
        unsafe_allow_html=True,
    )

    cols = st.columns(4)

    # ======================================================
    # CARD LOOP
    # ======================================================
    for idx, row in page_df.iterrows():
        with cols[idx % 4]:
        
            # --------------------------------
            # MEMORY READ (cheap, safe)
            # --------------------------------
            st.caption(f"🧠 RAM: {get_mem_mb():.0f} MB")

            player = row.get("player") or ""
            pretty_market = MARKET_DISPLAY_MAP.get(row.get("market"), row.get("market"))
            bet_type = str(row.get("bet_type")).upper()
            line = row.get("line")

            hit_val = row.get(hit_rate_col) or 0.0
            l5_avg = get_rolling_avg(row, 5)
            l10_avg = get_rolling_avg(row, 10)
            l20_avg = get_rolling_avg(row, 20)

            opp_rank = get_opponent_rank(row)
            rank_display = opp_rank if isinstance(opp_rank, int) else "-"
            rank_color = rank_to_color(opp_rank) if isinstance(opp_rank, int) else "#9ca3af"
           
            stat_prefix = detect_stat(row.get("market"))



            spark_key = (
                row.get("player"),
                row.get("market"),
                row.get("line"),
                row.get("game_id"),
            )
            spark_html = sparkline_map.get(spark_key, "")

            home_logo = TEAM_LOGOS_BASE64.get(
                normalize_team_code(row.get("player_team", "")), ""
            )
            opp_logo = TEAM_LOGOS_BASE64.get(
                normalize_team_code(row.get("opponent_team", "")), ""
            )

            # -------------------------
            # Book prices
            # -------------------------
            book_lines = []

            for bp in row.get("book_prices", []):
                logo = SPORTSBOOK_LOGOS_BASE64.get(bp.get("book"))
                price = bp.get("price")
                if logo and price is not None:
                    book_lines.append(
                        f"<div style='display:flex; align-items:center; gap:6px;'>"
                        f"<img src='{logo}' style='height:22px;' />"
                        f"<div style='font-size:0.75rem;font-weight:800;'>{price:+d}</div>"
                        f"</div>"
                    )

            books_html = (
                f"<div style='display:flex; flex-direction:column; align-items:flex-end; gap:4px;'>"
                f"{''.join(book_lines)}"
                f"</div>"
            )

            # -------------------------
            # BASE CARD
            # -------------------------
            base_card_html = (
                f"<div class='prop-card'>"
                f"<div style='display:flex; justify-content:space-between; align-items:center;'>"
                f"<div style='display:flex; align-items:center; gap:6px;'>"
                f"<img src='{home_logo}' style='height:20px;border-radius:4px;' />"
                f"<span style='font-size:0.7rem;color:#9ca3af;'>vs</span>"
                f"<img src='{opp_logo}' style='height:20px;border-radius:4px;' />"
                f"</div>"
                f"<div style='text-align:center; flex:1;'>"
                f"<div style='font-size:1.05rem;font-weight:700;'>{player}</div>"
                f"<div style='font-size:0.82rem;color:#9ca3af;'>{pretty_market} • {bet_type} {line}</div>"
                f"</div>"
                f"{books_html}"
                f"</div>"
                f"<div style='display:flex; justify-content:center; margin:8px 0;'>{spark_html}</div>"
                f"<div class='prop-meta'>"
                f"<div><div style='font-size:0.8rem;'>{hit_label}: {hit_val:.0%}</div>"
                f"<div style='font-size:0.7rem;'>L10 Avg: {_fmt1(l10_avg)}</div></div>"
                f"<div><div style='font-size:0.8rem;font-weight:700;color:{rank_color};'>{rank_display}</div>"
                f"<div style='font-size:0.7rem;'>Opp Rank</div></div>"
                f"</div>"
                f"</div>"
            )

            # -------------------------
            # EXPANDED ANALYTICS
            # -------------------------
            expanded_html = (
                f"<div class='expanded-wrap'>"

                # ==================================================
                # ROW 1 — AVERAGES
                # ==================================================
                f"<div class='expanded-row'>"
                f"<div class='metric'><span>L5</span><strong>{_fmt1(l5_avg)}</strong></div>"
                f"<div class='metric'><span>L10</span><strong>{_fmt1(l10_avg)}</strong></div>"
                f"<div class='metric'><span>L20</span><strong>{_fmt1(l20_avg)}</strong></div>"
                f"<div class='metric'><span>Δ Line</span><strong>{_fmt1((l10_avg - line) if l10_avg is not None and line is not None else None)}</strong></div>"
                f"</div>"

                # ==================================================
                # ROW 2 — L20 DISTRIBUTION
                # ==================================================
                f"<div class='expanded-row dist-row'>"
                f"<div class='metric'><span>L20 Hit</span><strong>{_pct(row.get('dist20_hit_rate'))}</strong></div>"
                f"<div class='metric'><span>+1</span><strong>{_pct(row.get('dist20_clear_1p_rate'))}</strong></div>"
                f"<div class='metric'><span>+2</span><strong>{_pct(row.get('dist20_clear_2p_rate'))}</strong></div>"
                f"<div class='metric'><span>Bad</span><strong>{_pct(row.get('dist20_fail_bad_rate'))}</strong></div>"
                f"<div class='metric'><span>Margin</span><strong>{_fmt1(row.get('dist20_avg_margin'))}</strong></div>"
                f"</div>"

                # ==================================================
                # ROW 3 — L40 DISTRIBUTION
                # ==================================================
                f"<div class='expanded-row dist-row'>"
                f"<div class='metric'><span>L40 Hit</span><strong>{_pct(row.get('dist40_hit_rate'))}</strong></div>"
                f"<div class='metric'><span>+1</span><strong>{_pct(row.get('dist40_clear_1p_rate'))}</strong></div>"
                f"<div class='metric'><span>+2</span><strong>{_pct(row.get('dist40_clear_2p_rate'))}</strong></div>"
                f"<div class='metric'><span>Bad</span><strong>{_pct(row.get('dist40_fail_bad_rate'))}</strong></div>"
                f"<div class='metric'><span>Margin</span><strong>{_fmt1(row.get('dist40_avg_margin'))}</strong></div>"
                f"</div>"

                # ==================================================
                # ROW 4 — INJURY / WOWY
                # ==================================================
                f"<div class='expanded-row wowy-row'>"
                f"{format_wowy_html(row.get('breakdown'), stat_prefix)}"
                f"</div>"

                f"</div>"
            )

            
            # -------------------------
            # SAVE BET (CONSTANT-MEMORY)
            # -------------------------
            if st.button("💾 Save Bet", key=f"save_{row['player']}_{row['market']}_{row['line']}_{row['bet_type']}"):
                ok = save_bet_simple(
                    player=row["player"],
                    market=row["market"],
                    line=row["line"],
                    price=row["price"],
                    bet_type=row["bet_type"],
                )
                if ok:
                    st.toast("Saved ✅")
                else:
                    st.toast("Already saved")


            # -------------------------
            # FULL CARD
            # -------------------------
            st.markdown(
                f"<details class='prop-card-wrapper'>"
                f"<summary>{base_card_html}<div class='expand-hint'>Click to expand ▾</div></summary>"
                f"<div class='card-expanded'>{expanded_html}</div>"
                f"</details>",
                unsafe_allow_html=True,
            )

    st.markdown(f"</div>", unsafe_allow_html=True)



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
    if show_only_saved_local and st.session_state.saved_bets:
        saved_keys = {
            (
                b["player"],
                b["market"],
                b["line"],
                b["bet_type"],
            )
            for b in st.session_state.saved_bets
        }
    
        d = d[
            d.apply(
                lambda r: (
                    r["player"],
                    r["market"],
                    r["line"],
                    r["bet_type"],
                ) in saved_keys,
                axis=1,
            )
        ]

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
def render_saved_bets_tab(user_id: int):
    st.subheader("Saved Bets")

    rows = get_saved_bets_for_user(user_id)

    if not rows:
        st.info("No saved bets yet.")
        return

    for player, market, line, bet_type in rows:
        st.markdown(
            f"""
            **{player}**  
            {market} **{bet_type} {line}**
            """
        )
        st.divider()

    # ---------------------------
    # Display saved bets
    # ---------------------------
    for bet in bets:
        st.markdown(
            f"""
            **{bet['player']}**  
            {bet['market']} **{bet['bet_type']} {bet['line']}**
            """
        )
        st.divider()

    # ---------------------------
    # Export (Pikkit-friendly)
    # ---------------------------
    export_txt = "\n".join(
        f"{b['player']} — {b['bet_type']} {b['line']} ({b['market']})"
        for b in bets
    )

    st.text_area(
        "Copy for Pikkit",
        export_txt,
        height=200,
    )

# ------------------------------------------------------
# TABS — NBA / NCAA + UNIVERSAL SAVED BETS
# ------------------------------------------------------

if sport == "NBA":
    # Saved Bets moved to LAST position in the bar
    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "📈 Props",
            "🏀 Game Lines",
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
        df = props_df.copy()

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

        game_analytics_df = load_game_analytics()
        game_report_df = load_game_report()
        game_odds_df = load_game_odds()
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
            
            def safe_odds_part(text):
                if not text:
                    return ""
                for part in str(text).split():
                    if part.startswith(("+", "-")) and part[1:].isdigit():
                        return part
                return ""

            # --- COLLAPSED (compact, SAFE) ---

            home_ml_price = safe_odds_part(home_ml_text)
            away_ml_price = safe_odds_part(away_ml_text)

            collapsed_home_ml = f"{home_abbr} {home_ml_price}".strip()
            collapsed_away_ml = f"{away_abbr} {away_ml_price}".strip()

            spread_part = (
                str(spread_text).split()[-1]
                if spread_text and str(spread_text).split()
                else ""
            )
            collapsed_spread = f"{home_abbr} {spread_part}".strip()

            total_part = (
                str(total_text)
                .replace("O/U", "")
                .replace("o/u", "")
                .strip()
                if total_text
                else ""
            )
            collapsed_total = f"O/U {total_part}".strip()

            
            # -------------------------------
            # GAME CARD (SAFE, NO JS)
            # -------------------------------
            
            st.markdown(
                f"""
                <style>
                .game-card {{
                    background: linear-gradient(145deg, #0f172a, #1e293b);
                    border-radius: 18px;
                    padding: 16px;
                    margin-bottom: 12px;
                    color: white;
                    border: 1px solid rgba(255,255,255,0.06);
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
            
                .summary-row {{
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 12px;
                    margin-top: 10px;
                    padding-top: 8px;
                    border-top: 1px solid rgba(255,255,255,0.08);
                    font-size: 0.75rem;
                }}
            
                .summary-title {{
                    color: #94a3b8;
                    font-size: 0.65rem;
                    letter-spacing: 0.05em;
                    text-transform: uppercase;
                    margin-bottom: 2px;
                }}
            
                .summary-line {{
                    display: flex;
                    justify-content: space-between;
                    gap: 6px;
                    white-space: nowrap;
                }}
            
                .expand-hint {{
                    color: #94a3b8;
                    font-size: 0.75rem;
                    text-align: center;
                    margin-top: 10px;
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
                </style>
                """,
                unsafe_allow_html=True,
            )
            
            # -------------------------------
            # COLLAPSED CARD
            # -------------------------------
            st.markdown(
                f"""
                <div class="game-card">
                    <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                        <div class="team-col">
                            <img src="{home_logo}" width="42" style="border-radius:6px;">
                            <div style="font-weight:700;margin-top:4px;">{home}</div>
                            <div class="proj-pts">{home_pts} pts</div>
                        </div>
            
                        <div class="center-col">
                            <div style="font-size:1.25rem;font-weight:700;">vs</div>
                            <div style="margin-top:4px;font-size:0.85rem;color:#9ca3af;">
                                {start_time}
                            </div>
                        </div>
            
                        <div class="team-col">
                            <img src="{away_logo}" width="42" style="border-radius:6px;">
                            <div style="font-weight:700;margin-top:4px;">{away}</div>
                            <div class="proj-pts">{away_pts} pts</div>
                        </div>
                    </div>
            
                    <div class="summary-row">
                        <div>
                            <div class="summary-title">Model</div>
                            <div class="summary-line"><span>Total</span><span>{tot_pts}</span></div>
                            <div class="summary-line"><span>Spread</span><span>{margin}</span></div>
                            <div class="summary-line"><span>ML</span><span>{home_win}%–{away_win}%</span></div>
                        </div>
            
                        <div>
                            <div class="summary-title">Market</div>
                            <div class="summary-line"><span>Total</span><span>{collapsed_total}</span></div>
                            <div class="summary-line"><span>Spread</span><span>{collapsed_spread}</span></div>
                            <div class="summary-line">
                                <span>ML</span><span>{collapsed_home_ml} / {collapsed_away_ml}</span>
                            </div>
                        </div>
                    </div>
            
                    <div class="expand-hint">Tap below to expand ↓</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            
            # -------------------------------
            # EXPANDED CONTENT (STREAMLIT-NATIVE)
            # -------------------------------
            with st.expander("Expanded details", expanded=False):
                st.markdown(
                    f"""
                    <div style="display:flex;gap:18px;flex-wrap:wrap;">
            
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
                    """,
                    unsafe_allow_html=True,
                )
    
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
    # TAB 3 — TREND LAB (same as your old Tab 2)
    # ------------------------------------------------------
    with tab3:
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

    with tab4:
        st.header("Saved Bets")
    
        saved = st.session_state.get("saved_bets", [])
    
        if not saved:
            st.info("You haven't saved any bets yet.")
        else:
            st.markdown("### 🧾 Your Saved Bets")
    
            for i, bet in enumerate(saved, start=1):
                st.markdown(
                    f"""
                    **{i}. {bet['player']}**  
                    Market: {bet['market']}  
                    {bet['bet_type'].upper()} {bet['line']}  
                    Price: {bet['price']}
                    ---
                    """
                )

        

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
        st.text_area(
            "Pikkit Export",
            pikkit_text,
            key="pikkit_textbox",
            height=120,
        )

        st.button(
            "📋 Copy",
            help="Click inside the text box, then press Ctrl+C / Cmd+C",
        )


        # --------------------------------------------------
        # OPEN PIKKIT BUTTON (Universal Link)
        # --------------------------------------------------
        st.link_button("📲 Open Pikkit", "https://quickpick.pikkit.com")


# ------------------------------------------------------
# LAST UPDATED FOOTER
# ------------------------------------------------------
now = datetime.now(EST)
st.sidebar.markdown(f"**Last Updated:** {now.strftime('%b %d, %I:%M %p')} ET")
