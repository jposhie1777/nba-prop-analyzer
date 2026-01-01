import os
os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"] = "none"

# ------------------------------------------------------
# NBA Prop Analyzer - Merged Production + Dev UI
# ------------------------------------------------------
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

import psutil  # must be before memory helpers
import math
from goat_auth import call_goat


# ======================================================
# MEMORY TRACKING HELPERS (DEFINE BEFORE CALLING)
# ======================================================
def get_rss_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / 1e6

def init_memory_state():
    if "mem_last_mb" not in st.session_state:
        st.session_state.mem_last_mb = get_rss_mb()
    if "mem_peak_mb" not in st.session_state:
        st.session_state.mem_peak_mb = st.session_state.mem_last_mb
    if "mem_render_peak_mb" not in st.session_state:
        st.session_state.mem_render_peak_mb = st.session_state.mem_last_mb

def record_memory_checkpoint():
    current = get_rss_mb()
    st.session_state.mem_peak_mb = max(st.session_state.mem_peak_mb, current)
    st.session_state.mem_render_peak_mb = max(st.session_state.mem_render_peak_mb, current)
    return current

def finalize_render_memory():
    current = get_rss_mb()
    last = st.session_state.mem_last_mb
    delta = current - last
    st.session_state.mem_last_mb = current
    st.session_state.mem_render_peak_mb = current
    return current, delta


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

st.sidebar.markdown("🧪 DEV_APP.PY RUNNING")

IS_DEV = True

# ------------------------------------------------------
# MEMORY STATE INIT (NOW SAFE)
# ------------------------------------------------------
init_memory_state()


# ======================================================
# DEV ACCESS CONTROL (EARLY)
# ======================================================
DEV_EMAILS = {
    "benvrana@bottleking.com",
    "jposhie1777@gmail.com",
}

def get_user_email():
    # 1️⃣ Explicit DEV override
    if IS_DEV:
        return "benvrana@bottleking.com"

    # 2️⃣ Streamlit hosted auth (prod)
    try:
        email = st.experimental_user.email
        if email:
            return email
    except Exception:
        pass

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
# SIDEBAR: DEV NAV ENTRY (SAFE)
# ------------------------------------------------------
if IS_DEV and is_dev_user():
    st.sidebar.divider()
    st.sidebar.markdown("### ⚙️ Dev Tools")

    if st.sidebar.button("Open Dev Panel"):
        st.query_params["tab"] = "dev"
        st.rerun()

# ------------------------------------------------------
# DEV-SAFE BIGQUERY and GAS CONSTANTS
# ------------------------------------------------------
DEV_BQ_DATASET = os.getenv("BIGQUERY_DATASET", "nba_prop_analyzer")

DEV_SP_TABLES = {
    "Game Analytics": "game_analytics",
    "Game Report": "game_report",
    "Historical Player Stats (Trends)": "historical_player_stats_for_trends",
    "Today's Props – Enriched": "todays_props_enriched",
    "Today's Props – Hit Rates": "todays_props_hit_rates",
}

# ------------------------------------------------------
# DEV: GOAT BIGQUERY TABLES (SCHEMA ONLY)
# ------------------------------------------------------
DEV_GOAT_TABLES = {
    "GOAT – Core Reference": {
        "Active Players": {
            "dataset": "nba_goat_data",
            "table": "active_players",
        },
    },
    "GOAT – Player Game Stats": {
        "Player Game Stats (Full)": {
            "dataset": "nba_goat_data",
            "table": "player_game_stats_full",
        },
        "Player Game Stats (Period)": {
            "dataset": "nba_goat_data",
            "table": "player_game_stats_period",
        },
        "Player Game Stats (Advanced)": {
            "dataset": "nba_goat_data",
            "table": "player_game_stats_advanced",
        },
    },
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
    client = get_dev_bq_client()   # 👈 THIS is the missing line
    df = client.query(sql).to_dataframe()
    df.flags.writeable = False
    return df

@st.cache_data(ttl=300)
def load_projected_starting_lineups_for_teams(team_abbrs: list[str]):
    team_list = ",".join(f"'{t}'" for t in team_abbrs)

    query = f"""
    SELECT
      team_abbr,
      player,
      player_id,
      projected_lineup_spot AS lineup_slot,
      projection_reason,
      starter_score,
      starter_pct,
      avg_minutes,
      rotation_tier,
      projected_at
    FROM `nba_goat_data.projected_starting_lineups`
    WHERE team_abbr IN ({team_list})
    ORDER BY team_abbr, projected_lineup_spot
    """
    return load_bq_df(query)


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
            params={          # 👈 ADD THIS
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
    FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table}'
    ORDER BY ordinal_position
    """

    df = load_bq_df(query)
    df.flags.writeable = False
    return df

# ======================================================
# DEV: INGEST STATE VIEW
# ======================================================
@st.cache_data(ttl=120, show_spinner=False)
def load_ingest_state() -> pd.DataFrame:
    sql = """
    SELECT
        job_name,
        last_run_ts,
        meta
    FROM `nba_goat_data.ingest_state`
    ORDER BY last_run_ts DESC
    """
    df = load_bq_df(sql)

    if df.empty:
        return df

    # Parse meta JSON cleanly
    def parse_meta(x):
        if x is None:
            return {}
        if isinstance(x, dict):
            return x
        try:
            return json.loads(x)
        except Exception:
            return {}

    df["meta"] = df["meta"].apply(parse_meta)

    # Optional: extract common fields for display
    df["date"] = df["meta"].apply(lambda m: m.get("date"))
    df["games"] = df["meta"].apply(lambda m: m.get("games") or m.get("games_checked"))
    df["rows"] = df["meta"].apply(lambda m: m.get("rows"))

    df.flags.writeable = False
    return df

# ======================================================
# DEV: QUERY REGISTRY (QUERY HEALTH)
# ======================================================
@st.cache_data(ttl=120, show_spinner=False)
def load_query_registry(domain: str | None = None) -> pd.DataFrame:
    where = ""
    if domain:
        where = f"WHERE domain = '{domain}'"

    sql = f"""
    SELECT
        query_id,
        query_name,
        domain,
        status,
        last_run_ts,
        expected_frequency_mins,
        target_table
    FROM `graphite-flare-477419-h7.ops.query_registry`
    {where}
    ORDER BY query_name
    """
    df = load_bq_df(sql)
    df.flags.writeable = False
    return df

# ======================================================
# DEV: INGEST STATE HELPERS
# ======================================================
def minutes_since(ts):
    if ts is None or pd.isna(ts):
        return None
    return (datetime.utcnow() - ts.replace(tzinfo=None)).total_seconds() / 60.0


def stale_style(val):
    if val is None:
        return ""
    # > 120 min = stale
    if val > 120:
        return "color:#ef4444;font-weight:700;"
    # 60–120 min = warning
    if val > 60:
        return "color:#f59e0b;font-weight:600;"
    return "color:#22c55e;"

def minutes_ago(ts):
    if ts is None or pd.isna(ts):
        return None
    return (datetime.utcnow() - ts.replace(tzinfo=None)).total_seconds() / 60.0


QUERY_STATUS_ICON = {
    "healthy": "🟢",
    "stale": "🟠",
    "never_run": "⚫",
}

# ======================================================
# DEV: BigQuery Stored Procedure Trigger (SAFE)
# ======================================================
def trigger_bq_procedure(proc_name: str):
    try:
        client = get_dev_bq_client()
        sql = f"CALL `nba_goat_data.{proc_name}`()"
        job = client.query(sql)
        job.result()
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

def render_query_health_panel(domain="goat"):
    st.subheader("📊 Query Health")

    df = load_query_registry(domain)

    if df.empty:
        st.info("No queries registered.")
        return

    df = df.copy()
    df["mins_ago"] = df["last_run_ts"].apply(minutes_ago)
    df["status_icon"] = df["status"].map(QUERY_STATUS_ICON)

    display_df = df[
        [
            "status_icon",
            "query_name",
            "mins_ago",
            "expected_frequency_mins",
            "target_table",
        ]
    ].rename(
        columns={
            "status_icon": "",
            "query_name": "Query",
            "mins_ago": "Last Run (min ago)",
            "expected_frequency_mins": "Expected (min)",
            "target_table": "Target Table",
        }
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

    st.caption("🟢 healthy • 🟠 stale • ⚫ never run")

# ======================================================
# DEV PAGE OVERRIDE (CRASH-SAFE)
# ======================================================
def render_dev_page():
    st.title("⚙️ DEV CONTROL PANEL")

    # --------------------------------------------------
    # NAV
    # --------------------------------------------------
    if st.button("⬅ Back to Main App", use_container_width=False):
        st.session_state["pending_tab"] = "main"

    st.caption("Always available • restricted access")
    st.markdown(f"**Email:** `{get_user_email()}`")

    st.divider()

    # ==================================================
    # 📡 INGESTION STATUS (GOAT)
    # ==================================================
    st.subheader("📡 Ingestion Status (GOAT)")

    try:
        ingest_df = load_ingest_state()

        if ingest_df.empty:
            st.warning("No ingestion state rows found.")
        else:
            ingest_df = ingest_df.copy()
            ingest_df["mins_ago"] = ingest_df["last_run_ts"].apply(minutes_since)

            display_df = ingest_df[
                ["job_name", "last_run_ts", "mins_ago", "date", "games", "rows"]
            ]

            st.dataframe(
                display_df.style.applymap(
                    stale_style,
                    subset=["mins_ago"],
                ),
                use_container_width=True,
                hide_index=True,
            )

            st.caption(
                "🟢 < 60 min • 🟠 60–120 min • 🔴 > 120 min since last successful run\n\n"
                "Timestamps are UTC. Fields are parsed from ingest_state.meta."
            )

    except Exception as e:
        st.error("❌ Failed to load ingestion status")
        st.code(str(e))

    st.divider()

    # ==================================================
    # 📊 QUERY HEALTH (GOAT)
    # ==================================================
    try:
        render_query_health_panel(domain="goat")
    except Exception as e:
        st.error("❌ Failed to load query health")
        st.code(str(e))

    # ==================================================
    # BIGQUERY — STORED PROCEDURE TRIGGERS
    # ==================================================
    st.subheader("🧪 BigQuery — Manual Stored Procedure Triggers")

    BQ_PROCS = [
        ("🐐 GOAT Daily Pipeline (ALL)", "run_daily_goat_pipeline"),
    ]
    
    for label, proc in BQ_PROCS:
        c1, c2 = st.columns([3, 1])
    
        with c1:
            st.markdown(f"**{label}**")
            st.caption(f"`{DEV_BQ_DATASET}.{proc}`")
    
        with c2:
            if st.button("▶ Run", key=f"run_{proc}", use_container_width=True):
                with st.spinner(f"Running {proc}…"):
                    trigger_bq_procedure(proc)
    
    st.divider()
    

    # ==================================================
    # ☁️ GOAT Cloud Run Jobs
    # ==================================================
    GOAT_JOBS = {
        # ----------------------------
        # Core / Frequent
        # ----------------------------
        "Player Props": (
            "https://goat-ingestion-763243624328.us-central1.run.app/goat/ingest/player-props",
            {"bypass": True},
        ),
        "Player Injuries": (
            "https://goat-ingestion-763243624328.us-central1.run.app/goat/ingest/player-injuries",
            {},
        ),
    
        # ----------------------------
        # Games
        # ----------------------------
        "Games Today": (
            "https://goat-ingestion-763243624328.us-central1.run.app/goat/ingest/games",
            {"date": "today", "bypass": True},
        ),
        "Games Yesterday": (
            "https://goat-ingestion-763243624328.us-central1.run.app/goat/ingest/games",
            {"date": "yesterday", "bypass": True},
        ),
    
        # ----------------------------
        # Stats Pipelines
        # ----------------------------
        "Stats — Advanced": (
            "https://goat-ingestion-763243624328.us-central1.run.app/goat/ingest/stats/advanced",
            {"bypass": True},
        ),
        "Stats — Full": (
            "https://goat-ingestion-763243624328.us-central1.run.app/goat/ingest/stats/full",
            {},
        ),
        "Stats — Quarters": (
            "https://goat-ingestion-763243624328.us-central1.run.app/goat/ingest/stats/quarters",
            {},
        ),
    
        # ----------------------------
        # Plays / Lineups
        # ----------------------------
        "Plays — First 3 Min": (
            "https://goat-ingestion-763243624328.us-central1.run.app/goat/ingest/plays/first3min",
            {"bypass": True},
        ),
        "Lineups — Yesterday": (
            "https://goat-ingestion-763243624328.us-central1.run.app/goat/ingest/lineups",
            {
                "start": (datetime.utcnow() - pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                "end": (datetime.utcnow() - pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            },
        ),
    }
    
    cols = st.columns(3)

    for i, (label, (url, params)) in enumerate(GOAT_JOBS.items()):
        with cols[i % 3]:
            if st.button(f"▶ {label}", use_container_width=True):
                with st.spinner(f"Triggering {label}…"):
                    try:
                        call_goat(url, params)
                        st.success("Triggered")
                    except Exception as e:
                        st.error("Failed")
                        st.code(str(e))
    
    st.caption("Secure direct trigger (same endpoints as Cloud Scheduler)")
    st.divider()

    def trigger_goat_job(job_key: str):
        try:
            url, params = GOAT_ENDPOINTS[job_key]
            call_goat(url, params)
            st.success(f"✅ GOAT job `{job_key}` triggered")
        except Exception as e:
            st.error("❌ GOAT job failed")
            st.code(str(e))

    # -------------------------------
    # GOAT Ingestion Tables (Schema Only)
    # -------------------------------
    st.markdown("### 🐐 GOAT Ingestion Tables")

    for group, tables in DEV_GOAT_TABLES.items():
        st.markdown(f"**{group}**")

        for label, meta in tables.items():
            dataset = meta["dataset"]
            table = meta["table"]

            with st.expander(f"📄 {label}", expanded=False):
                st.code(f"{dataset}.{table}", language="text")

                try:
                    schema_df = get_table_schema(dataset, table)

                    if schema_df.empty:
                        st.warning("No columns found (table may not exist yet).")
                    else:
                        st.dataframe(
                            schema_df,
                            use_container_width=True,
                            hide_index=True,
                        )

                except Exception as e:
                    st.error("Failed to load schema")
                    st.code(str(e))

    st.divider()

    # ==================================================
    # GOOGLE SHEETS — SANITY CHECKS
    # ==================================================
    st.subheader("📊 Google Sheet Sanity Checks")

    SHEET_ID = "1p_rmmiUgU18afioJJ3jCHh9XeX7V4gyHd_E0M3A8M3g"

    # -------------------------------
    # Odds Sheet
    # -------------------------------
    try:
        odds_rows = read_sheet_values(SHEET_ID, "Odds!A:I")
        has_rows = len(odds_rows) > 1

        labels = [
            (r[8] or "").strip().lower()
            for r in odds_rows[1:]
            if len(r) >= 9
        ] if has_rows else []

        st.markdown("**Odds Tab**")

        if has_rows:
            st.success("✅ Rows exist after header")
        else:
            st.error("❌ No rows found after header")

        if any("over" in l for l in labels) and any("under" in l for l in labels):
            st.success("✅ Both Over and Under found")
        elif any("over" in l for l in labels):
            st.warning("⚠️ Only Over found")
        elif any("under" in l for l in labels):
            st.warning("⚠️ Only Under found")
        else:
            st.error("❌ No Over / Under values found")

    except Exception as e:
        st.error("❌ Failed to read Odds tab")
        st.code(str(e))

    # -------------------------------
    # Game Odds Sheet
    # -------------------------------
    try:
        game_odds_rows = read_sheet_values(SHEET_ID, "Game Odds Sheet!A:A")
        has_rows = len(game_odds_rows) > 1

        st.markdown("**Game Odds Sheet**")

        if has_rows:
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

DATASET = "nba_goat_data"

PROPS_TABLE_FULL = "props_full_enriched"
PROPS_TABLE_Q1   = "props_q1_enriched"


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

# ------------------------------------------------------
# LOCKED THEME (STATIC) AND GLOBAL STYLES
# ------------------------------------------------------
THEME_BG = "#020617"
THEME_ACCENT = "#0ea5e9"
THEME_ACCENT_SOFT = "#0369a1"

@st.cache_resource
def load_static_ui():
    st.markdown(
        """
        <style>

        /* ==================================================
           GLOBAL IMAGE SAFETY CLAMP (CRITICAL)
           Prevents runaway ESPN / SVG logos
        ================================================== */
        img {
            max-width: 32px !important;
            max-height: 32px !important;
            width: auto !important;
            height: auto !important;
            object-fit: contain !important;
            display: inline-block;
        }

        /* ==================================================
           EXPAND / COLLAPSE WRAPPER (SHARED)
        ================================================== */
        .prop-card-wrapper {
            position: relative;
            z-index: 5;
            border-radius: 16px;
        }

        .prop-card-wrapper summary {
            cursor: pointer;
            list-style: none;
        }

        .prop-card-wrapper summary::-webkit-details-marker {
            display: none;
        }

        /* Disable pointer events ONLY for collapsed summary content */
        .prop-card-wrapper summary > * {
            pointer-events: none;
        }

        .prop-card-wrapper .card-expanded,
        .prop-card-expanded {
            margin-top: 6px;
            pointer-events: auto;
        }

        .expand-hint {
            text-align: center;
            font-size: 0.65rem;
            opacity: 0.55;
            margin-top: 6px;
        }

        /* ==================================================
           BASE CARD (PROPS / LINEUPS / FIRST BASKET)
        ================================================== */
        .prop-card,
        .prop-card-wrapper summary {
            background: linear-gradient(
                180deg,
                rgba(15, 23, 42, 0.92),
                rgba(2, 6, 23, 0.95)
            );
            border: none;
            border-radius: 16px;
            padding: 16px 18px;
            width: 100%;
            box-shadow:
                0 10px 28px rgba(0, 0, 0, 0.55),
                inset 0 1px 0 rgba(255, 255, 255, 0.04);
        }

        .prop-card-wrapper:hover summary {
            box-shadow:
                0 14px 36px rgba(0, 0, 0, 0.65),
                inset 0 1px 0 rgba(255, 255, 255, 0.06);
        }

        /* ==================================================
           CARD GRID (VERTICAL LAYOUT)
        ================================================== */
        .card-grid {
            display: grid;
            grid-template-rows: auto auto auto auto;
            row-gap: 10px;
        }

        /* ==================================================
           EXPANDED METRICS
        ================================================== */
        .expanded-wrap {
            background: rgba(255,255,255,0.03);
            padding: 10px;
            border-radius: 12px;
        }

        .expanded-row {
            display: flex;
            justify-content: space-between;
            gap: 8px;
        }

        .metric {
            flex: 1;
            text-align: center;
            font-size: 0.72rem;
        }

        .metric span {
            display: block;
            color: #9ca3af;
        }

        .metric strong {
            font-size: 0.85rem;
            font-weight: 700;
            color: #ffffff;
        }

        /* ==================================================
           MATCHUP HEADER (CENTERED)
        ================================================== */
        .matchup-header {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 14px;
            padding: 14px 18px;
            margin: 28px 0 14px;
            border-radius: 18px;
            background: linear-gradient(
                180deg,
                rgba(30, 41, 59, 0.9),
                rgba(2, 6, 23, 0.95)
            );
            box-shadow:
                0 12px 28px rgba(0,0,0,0.6),
                inset 0 1px 0 rgba(255,255,255,0.05);
        }

        .matchup-team {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 0.95rem;
            font-weight: 800;
            white-space: nowrap;
        }

        .matchup-team img {
            width: 22px !important;
            height: 22px !important;
        }

        .matchup-at {
            font-size: 0.9rem;
            opacity: 0.6;
        }

        .matchup-time {
            margin-left: 14px;
            font-size: 0.75rem;
            opacity: 0.6;
            white-space: nowrap;
        }

        /* ==================================================
           TEAM HEADER (LINEUPS)
        ================================================== */
        .team-header-card {
            display: flex;
            align-items: center;
            justify-content: center;   /* ✅ CENTER CONTENT HORIZONTALLY */
            gap: 10px;
            padding: 10px 16px;
            margin: 12px 0 8px;
            border-radius: 14px;
            background: rgba(255,255,255,0.04);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
            width: 100%;               /* ✅ ENSURE FULL-WIDTH FOR TRUE CENTERING */
        }


        .team-header-card img {
            width: 22px !important;
            height: 22px !important;
        }

        .team-header-name {
            font-size: 0.9rem;
            font-weight: 800;
            letter-spacing: 0.4px;
        }

        /* ==================================================
           LINEUP LIST
        ================================================== */
        .lineup-list {
            margin-top: 6px;
        }

        .lineup-player {
            font-size: 0.82rem;
            padding: 2px 0;
            line-height: 1.25;
        }

        .lineup-player.empty {
            opacity: 0.5;
        }

        .lineup-subtitle {
            margin-top: 8px;
            font-size: 0.72rem;
            opacity: 0.65;
        }

        .prop-card-title {
            font-size: 0.9rem;
            font-weight: 700;
            margin-bottom: 6px;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )

load_static_ui()

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

CODE_TO_TEAM_NAME = {v: k for k, v in TEAM_NAME_TO_CODE.items()}

def team_full_name(team_abbr: str) -> str:
    return CODE_TO_TEAM_NAME.get(team_abbr, team_abbr)


def logo(team_name: str) -> str:
    code = TEAM_NAME_TO_CODE.get(team_name)
    if not code:
        return "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"
    return TEAM_LOGOS.get(code)

# -------------------------------
# Sportsbook Logos
# -------------------------------
import base64
import pathlib

@st.cache_resource
def load_logo_base64(path: pathlib.Path) -> str:
    if not path.exists():
        return ""
    encoded = base64.b64encode(path.read_bytes()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"

FILE_DIR = pathlib.Path(__file__).resolve().parent
LOGO_DIR = FILE_DIR / "static" / "logos"

SPORTSBOOK_LOGOS = {
    "DraftKings": load_logo_base64(LOGO_DIR / "Draftkingssmall.png"),
    "FanDuel": load_logo_base64(LOGO_DIR / "Fanduelsmall.png"),
}

# -------------------------------
# Saved Bets (constant-memory)
# -------------------------------
MAX_SAVED_BETS = 150  # keep this small + stable

def _bet_key(player, market, line, bet_type):
    # minimal stable key — avoids duplicates + memory bloat
    return f"{player}|{market}|{line}|{bet_type}".lower().strip()

if "saved_bets" not in st.session_state:
    st.session_state.saved_bets = []

if "saved_bets_keys" not in st.session_state:
    st.session_state.saved_bets_keys = set()

if "saved_bets_text" not in st.session_state:
    st.session_state.saved_bets_text = []
    

PAGE_SIZE = 30

if "page" not in st.session_state:
    st.session_state.page = 0

# ------------------------------------------------------
# DATA: PROPS AND HISTOICAL STATS (minimal)
# ------------------------------------------------------
TRENDS_SQL = """
SELECT
  player,

  -- Core box score
  pts_last10_list,
  reb_last10_list,
  ast_last10_list,
  stl_last10_list,
  blk_last10_list,

  -- Combos
  pra_last10_list,
  pr_last10_list,
  pa_last10_list,
  ra_last10_list,

  -- Shooting / misc (new, safe to include)
  fgm_last10_list,
  fga_last10_list,
  fg3m_last10_list,
  fg3a_last10_list,
  ftm_last10_list,
  fta_last10_list,
  turnover_last10_list,
  pf_last10_list,

  last10_dates
FROM `nba_goat_data.historical_player_trends`
"""

COLUMN_REMAP = {
    "team": "player_team",
    "stat_type": "market",
    "prop_class": "bet_type",

    "hit_rate_l5": "hit_rate_last5",
    "hit_rate_l10": "hit_rate_last10",
    "hit_rate_l20": "hit_rate_last20",
    "hit_rate_l40": "hit_rate_last40",

    "implied_probability": "implied_prob",

    "clear_plus1_rate": "dist20_clear_1p_rate",
    "clear_plus2_rate": "dist20_clear_2p_rate",
    "bad_miss_rate_l20": "dist20_fail_bad_rate",
    "avg_margin_l20": "dist20_avg_margin",

    "bad_miss_rate_l40": "dist40_fail_bad_rate",
    "avg_margin_l40": "dist40_avg_margin",
}


@st.cache_data(ttl=1800, show_spinner=False)
def load_trends() -> pd.DataFrame:
    df = load_bq_df(TRENDS_SQL)
    df["player"] = df["player"].astype(str)
    df.flags.writeable = False
    return df

@st.cache_data(ttl=1800, show_spinner=False)
def load_trends_q1() -> pd.DataFrame:
    sql = """
    SELECT *
    FROM `nba_goat_data.historical_player_trends_q1`
    """
    df = load_bq_df(sql)
    df["player"] = df["player"].astype(str)
    df.flags.writeable = False
    return df

@st.cache_data(ttl=300, show_spinner=True)
def load_first_basket_today() -> pd.DataFrame:
    sql = """
    SELECT
        fb.*,
        g.home_team_abbr,
        g.away_team_abbr,
        t.tip_win_pct,
        t.jump_attempts
    FROM nba_goat_data.first_basket_projection_today fb
    JOIN nba_goat_data.games g
      ON fb.game_id = g.game_id
    LEFT JOIN nba_goat_data.tip_win_metrics t
      ON t.entity_type = 'team'
     AND t.team_abbr = fb.team_abbr
    WHERE fb.game_date = CURRENT_DATE("America/New_York")
    """
    return load_bq_df(sql)
    
@st.cache_data(ttl=300)
def load_team_most_used_lineups_for_teams(team_abbrs: list[str]):
    team_list = ",".join(f"'{t}'" for t in team_abbrs)

    query = f"""
    SELECT
      team_abbr,
      player,
      player_id,
      lineup_slot,
      times_used,
      first_used,
      last_used
    FROM `nba_goat_data.team_most_used_lineups`
    WHERE team_abbr IN ({team_list})
    ORDER BY team_abbr, lineup_slot
    """
    return load_bq_df(query)


@st.cache_data(ttl=300)
def load_todays_games():
    query = """
    SELECT
      game_id,
      home_team_abbr,
      away_team_abbr
    FROM `nba_goat_data.games`
    WHERE game_date = CURRENT_DATE()
      AND status != 'Final'
    ORDER BY start_time_est
    """
    return load_bq_df(query)


@st.cache_data(ttl=900, show_spinner=True)
def load_props(table_name: str) -> pd.DataFrame:
    # --------------------------------------------------
    # LOAD FROM BIGQUERY (DYNAMIC TABLE)
    # --------------------------------------------------
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{table_name}`
    """
    df = load_bq_df(sql)

    if df.empty:
        df.flags.writeable = False
        return df

    # --------------------------------------------------
    # NORMALIZE GOAT → APP SCHEMA
    # --------------------------------------------------
    df = df.rename(columns=COLUMN_REMAP)

    # --------------------------------------------------
    # MARKET NORMALIZATION (GOAT → APP)
    # --------------------------------------------------
    if "market" in df.columns:
        df["market"] = (
            df["market"]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({
                "PTS": "player_points",
                "REB": "player_rebounds",
                "AST": "player_assists",
                "STL": "player_steals",
                "BLK": "player_blocks",
                "DD":  "player_double_double",
                "TD":  "player_triple_double",
            })
        )

    # --------------------------------------------------
    # BOOKMAKER NORMALIZATION
    # --------------------------------------------------
    if "bookmaker" in df.columns:
        df["bookmaker"] = (
            df["bookmaker"]
            .astype(str)
            .str.strip()
            .str.lower()
            .replace({
                "draftkings": "DraftKings",
                "fanduel": "FanDuel",
            })
        )


    # --------------------------------------------------
    # CRITICAL SEMANTIC FIX
    # GOAT uses "Count" (binary props)
    # App expects Over / Under
    # --------------------------------------------------
    if "bet_type" in df.columns:
        df["bet_type"] = (
            df["bet_type"]
            .astype(str)
            .str.strip()
            .replace({"Count": "Over"})
        )

    # --------------------------------------------------
    # KEEP ONLY REQUIRED COLUMNS (MEMORY SAFE)
    # --------------------------------------------------
    keep = [
        # IDENTITY / ROUTING
        "player", "player_team",
        "home_team", "away_team",
        "market", "line", "bet_type",
        "bookmaker", "price",
        "game_date",

        # HIT RATES / EDGE
        "hit_rate_last5", "hit_rate_last10", "hit_rate_last20",
        "implied_prob",
        "edge_raw", "edge_pct",

        # SCALAR ROLLING AVERAGES
        "pts_last5", "pts_last10", "pts_last20",
        "reb_last5", "reb_last10", "reb_last20",
        "ast_last5", "ast_last10", "ast_last20",
        "stl_last5", "stl_last10", "stl_last20",
        "blk_last5", "blk_last10", "blk_last20",

        # COMBOS
        "pra_last5", "pra_last10", "pra_last20",
        "pr_last5",  "pr_last10",  "pr_last20",
        "pa_last5",  "pa_last10",  "pa_last20",
        "ra_last5",  "ra_last10",  "ra_last20",

        # OPPONENT / MATCHUP
        "opp_pos_pts_rank",
        "opp_pos_reb_rank",
        "opp_pos_ast_rank",
        "opp_pos_stl_rank",
        "opp_pos_blk_rank",
        "opp_pos_pra_rank",
        "opp_pos_pr_rank",
        "opp_pos_pa_rank",
        "opp_pos_ra_rank",
        
        "avg_stat_l5",
        "avg_stat_l10",
        "avg_stat_l20",

        # PROJECTION / CONFIDENCE
        "proj_last10",
        "proj_diff_vs_line",
        "proj_std_last10",
        "proj_volatility_index",
        "matchup_difficulty_by_stat",

        # DISTRIBUTION
        "dist20_hit_rate",
        "dist20_clear_1p_rate",
        "dist20_clear_2p_rate",
        "dist20_fail_bad_rate",
        "dist20_avg_margin",

        "dist40_hit_rate",
        "dist40_clear_1p_rate",
        "dist40_clear_2p_rate",
        "dist40_fail_bad_rate",
        "dist40_avg_margin",

        # MINUTES / ROLE
        "est_minutes",
        "delta_minutes",
        "usage_bump_pct",
    ]

    df = df[[c for c in keep if c in df.columns]].copy()

    # --------------------------------------------------
    # TYPE COERCION (SAFE)
    # --------------------------------------------------
    for c in ("price", "line"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in (
        "hit_rate_last5",
        "hit_rate_last10",
        "hit_rate_last20",
        "implied_prob",
        "edge_pct",
        "edge_raw",
    ):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    # --------------------------------------------------
    # STRING NORMALIZATION (LOW MEMORY)
    # --------------------------------------------------
    for c in (
        "player",
        "market",
        "bet_type",
        "bookmaker",
        "player_team",
        "home_team",
        "visitor_team",
        "opponent_team",
    ):
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)

    df.flags.writeable = False
    return df

# ------------------------------------------------------
# SAVED BETS (SESSION ONLY)
# ------------------------------------------------------
def _bet_key(player, market, line, bet_type) -> str:
    return f"{player}|{market}|{line}|{bet_type}".lower().strip()

def init_saved_bets_state():
    if "saved_bets" not in st.session_state:
        st.session_state.saved_bets = []
    if "saved_bets_keys" not in st.session_state:
        st.session_state.saved_bets_keys = set()

def save_bet_simple(player, market, line, price, bet_type) -> bool:
    init_saved_bets_state()
    bets = st.session_state.saved_bets
    keys = st.session_state.saved_bets_keys

    k = _bet_key(player, market, line, bet_type)
    if k in keys:
        return False

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
        keys.discard(_bet_key(old.get("player"), old.get("market"), old.get("line"), old.get("bet_type")))

    return True

def safe_team_logo(team_abbr: str | None) -> str:
    if not team_abbr:
        return "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"
    return TEAM_LOGOS.get(
        team_abbr,
        "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"
    )


if "_clipboard" in st.session_state:
    st.toast("Copied — paste into Gambly Bot 🤖")
    st.write(
        f"""
        <textarea id="clip" style="position:fixed;opacity:0;">
        {st.session_state["_clipboard"]}
        </textarea>
        <script>
        navigator.clipboard.writeText(
            document.getElementById("clip").value
        );
        </script>
        """,
        unsafe_allow_html=True,
    )
    del st.session_state["_clipboard"]

def render_saved_bets():
    if "saved_bets_text" not in st.session_state:
        st.session_state.saved_bets_text = []

    bets = st.session_state.saved_bets_text

    # -------------------------
    # HEADER + CLEAR BUTTON
    # -------------------------
    col1, col2 = st.columns([4, 1])

    with col1:
        st.subheader("📋 Saved Bets")
        st.caption("Session-only • copy & paste into Gambly")

    with col2:
        if st.button("🗑 Clear All", use_container_width=True):
            st.session_state.saved_bets_text.clear()
            st.toast("Cleared all saved bets")

    st.divider()

    # -------------------------
    # COPY AREA (MOVED UP)
    # -------------------------
    if not bets:
        st.info("No saved bets yet.")
        return

    st.code(
        "\n\n".join(bets),
        language="text",
    )

    if st.button("📋 Copy All for Gambly"):
        st.session_state["_clipboard"] = "\n\n".join(bets)

    st.divider()

    # -------------------------
    # 🤖 GAMBLy BOT (MOVED DOWN)
    # -------------------------
    st.markdown("### 🤖 Gambly Bot")
    st.link_button(
        "Open Gambly Bot",
        "https://www.gambly.com/gambly-bot",
    )
    st.caption("Paste the copied bets into Gambly Bot")

def render_first_basket_tab():
    st.subheader("🥇 First Basket Projections")

    df = load_first_basket_today()

    if df.empty:
        st.info("No first basket projections available.")
        return

    render_first_basket_cards(df)

# ------------------------------------------------------
# PROP CARD HELPERS
# ------------------------------------------------------
def compute_implied_prob(price) -> float | None:
    try:
        p = float(price)
    except Exception:
        return None
    if p == 0:
        return None
    # American odds
    if p < 0:
        return abs(p) / (abs(p) + 100.0)
    return 100.0 / (p + 100.0)

def fmt_pct(x) -> str:
    try:
        if x is None or pd.isna(x):
            return "—"
        return f"{float(x) * 100:.0f}%"
    except Exception:
        return "—"

def fmt_odds(x) -> str:
    try:
        if x is None or pd.isna(x):
            return "—"
        v = int(round(float(x)))
        return f"+{v}" if v > 0 else str(v)
    except Exception:
        return "—"

def fmt_num(x, d=1) -> str:
    try:
        if x is None or pd.isna(x):
            return "—"
        return f"{float(x):.{d}f}"
    except Exception:
        return "—"

def clamp(x, lo=0.0, hi=1.0):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo


def safe_div(n, d, default=0.0):
    try:
        return n / d if d else default
    except Exception:
        return default

def compute_confidence(
    row,
    *,
    hit_rate_col: str,      # e.g. "hit_rate_last10"
    stat_key: str,          # normalized stat key (points, steals, pra, etc.)
):
    """
    Returns:
        confidence_score (0–100),
        components dict (for debugging / tooltips)
    """

    components = {}

    # --------------------------------------------------
    # 1) EDGE SCORE (vs implied probability)
    # --------------------------------------------------
    hit = row.get(hit_rate_col)
    implied = row.get("implied_prob")

    if hit is not None and implied is not None:
        edge = hit - implied
        edge_score = clamp((edge + 0.05) / 0.25)
    else:
        edge_score = 0.0

    components["edge"] = edge_score

    # --------------------------------------------------
    # 2) STABILITY SCORE (L5 / L10 / L20 agreement)
    # --------------------------------------------------
    hr5 = row.get("hit_rate_last5")
    hr10 = row.get("hit_rate_last10")
    hr20 = row.get("hit_rate_last20")

    if hr5 is not None and hr10 is not None and hr20 is not None:
        spread = max(hr5, hr10, hr20) - min(hr5, hr10, hr20)
        stability_score = clamp(1.0 - spread * 2.0)
    else:
        stability_score = 0.5

    components["stability"] = stability_score

    # --------------------------------------------------
    # 3) PROJECTION VS LINE
    # --------------------------------------------------
    proj = row.get("proj_last10")
    diff = row.get("proj_diff_vs_line")

    if proj is not None and diff is not None:
        projection_score = clamp((diff + 1.0) / 4.0)
    else:
        projection_score = 0.5

    components["projection"] = projection_score

    # --------------------------------------------------
    # 4) VOLATILITY / RISK
    # --------------------------------------------------
    vol = row.get("proj_volatility_index")
    std = row.get("proj_std_last10")

    if vol is not None:
        volatility_score = clamp(1.0 - vol)
    elif std is not None:
        volatility_score = clamp(1.0 - safe_div(std, proj or 1.0))
    else:
        volatility_score = 0.5

    components["volatility"] = volatility_score

    # --------------------------------------------------
    # 5) MATCHUP QUALITY (FIXED OPP RANK LOGIC)
    # 1 = hardest, 30 = easiest
    # --------------------------------------------------
    opp_rank = row.get(f"opp_pos_{stat_key}_rank")

    if opp_rank is not None:
        matchup_score = clamp((opp_rank - 1) / 29.0)
    else:
        matchup_score = 0.5

    components["matchup"] = matchup_score

    # --------------------------------------------------
    # 6) MINUTES / ROLE CONFIDENCE
    # --------------------------------------------------
    est_min = row.get("est_minutes")
    delta_min = row.get("delta_minutes")

    if est_min is not None:
        minutes_score = clamp(est_min / 36.0)
        if delta_min is not None and delta_min < 0:
            minutes_score *= clamp(1.0 + delta_min / 10.0)
    else:
        minutes_score = 0.5

    components["minutes"] = minutes_score

    # --------------------------------------------------
    # 7) MOMENTUM REWARD (NEW)
    # --------------------------------------------------
    bad_miss = row.get("dist20_fail_bad_rate")

    momentum_bonus = 0.0
    if (
        hr20 is not None
        and bad_miss is not None
        and diff is not None
        and hr20 >= 0.95
        and bad_miss <= 0.05
        and diff >= 6
    ):
        momentum_bonus = 0.06  # +6 confidence points

    components["momentum"] = momentum_bonus

    # --------------------------------------------------
    # WEIGHTED COMBINATION
    # --------------------------------------------------
    weights = {
        "edge": 0.32,
        "stability": 0.20,
        "projection": 0.16,
        "volatility": 0.16,
        "matchup": 0.10,
        "minutes": 0.06,
    }

    confidence = sum(
        components[k] * weights[k] for k in weights
    )

    confidence += momentum_bonus

    confidence_score = round(clamp(confidence) * 100)

    return confidence_score, components

import json

import re

import numpy as np

def get_stat_avgs(row, stat_key):
    if stat_key == "points":
        return row.get("pts_last5"), row.get("pts_last10"), row.get("pts_last20")

    if stat_key == "rebounds":
        return row.get("reb_last5"), row.get("reb_last10"), row.get("reb_last20")

    if stat_key == "assists":
        return row.get("ast_last5"), row.get("ast_last10"), row.get("ast_last20")

    if stat_key == "steals":   # ✅ ADD
        return row.get("stl_last5"), row.get("stl_last10"), row.get("stl_last20")

    if stat_key == "blocks":   # ✅ ADD
        return row.get("blk_last5"), row.get("blk_last10"), row.get("blk_last20")

    if stat_key == "pra":
        return row.get("pra_last5"), row.get("pra_last10"), row.get("pra_last20")

    if stat_key == "points_assists":
        return row.get("pa_last5"), row.get("pa_last10"), row.get("pa_last20")

    if stat_key == "points_rebounds":
        return row.get("pr_last5"), row.get("pr_last10"), row.get("pr_last20")

    if stat_key == "rebounds_assists":
        return row.get("ra_last5"), row.get("ra_last10"), row.get("ra_last20")

    return None, None, None

def handle_save_bet(bet_line: str):
    if "saved_bets_text" not in st.session_state:
        st.session_state.saved_bets_text = []

    if bet_line not in st.session_state.saved_bets_text:
        st.session_state.saved_bets_text.append(bet_line)

def coerce_numeric_list(val):
    if val is None:
        return []

    # ✅ HANDLE NUMPY ARRAYS (THIS IS THE FIX)
    if isinstance(val, np.ndarray):
        return [float(v) for v in val if isinstance(v, (int, float, np.number))]

    if isinstance(val, list):
        return [float(v) for v in val if isinstance(v, (int, float))]

    if isinstance(val, str):
        # handle BigQuery array string like "array([1., 2., 3.])"
        if val.startswith("array("):
            try:
                inner = val.replace("array(", "").rstrip(")")
                return [float(v) for v in inner.strip("[]").split(",")]
            except Exception:
                return []

        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                return [float(v) for v in parsed if v is not None]
        except Exception:
            pass

        try:
            return [float(v) for v in val.split(",") if v.strip()]
        except Exception:
            return []

    return []

    if val is None or pd.isna(val):
        return []

    # Already a Python list
    if isinstance(val, list):
        return [float(v) for v in val if isinstance(v, (int, float))]

    # NumPy array
    if hasattr(val, "tolist"):
        return [float(v) for v in val.tolist()]

    # String case (BigQuery often returns ARRAYs like this)
    if isinstance(val, str):
        s = val.strip()

        # Handle: array([1., 2., 3.])
        if s.lower().startswith("array"):
            nums = re.findall(r"-?\d+\.?\d*", s)
            return [float(n) for n in nums]

        # Handle: [1, 2, 3]
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [float(v) for v in parsed if v is not None]
            except Exception:
                pass

    return []

def normalize_market_key(market: str) -> str:
    m = (market or "").lower()

    # strip known wrappers
    m = m.replace("player_", "")
    m = m.replace("_alternate", "")

    # normalize combos first
    if "points_rebounds_assists" in m:
        return "pra"
    if "points_assists" in m:
        return "points_assists"
    if "points_rebounds" in m:
        return "points_rebounds"
    if "rebounds_assists" in m:
        return "rebounds_assists"

    # singles
    if "points" in m:
        return "points"
    if "rebounds" in m:
        return "rebounds"
    if "assists" in m:
        return "assists"
    if "steals" in m:
        return "steals"        # 👈 ADD
    if "blocks" in m:
        return "blocks"

    return ""

def normalize_lineup_players(players):
    """
    Safely normalize BigQuery ARRAY / numpy / JSON into list[str]
    """
    if players is None:
        return []

    # NumPy array or pandas Series
    if hasattr(players, "tolist"):
        players = players.tolist()

    # JSON string
    if isinstance(players, str):
        try:
            parsed = json.loads(players)
            if isinstance(parsed, list):
                return [str(p) for p in parsed]
        except Exception:
            return []

    # Python list
    if isinstance(players, list):
        return [str(p) for p in players if p]

    return []

def get_l10_values(row, *, market_window: str):
    key = normalize_market_key(row.get("market"))

    # -----------------------
    # Q1 PROPS
    # -----------------------
    if market_window == "Q1":
        if key == "points":
            return coerce_numeric_list(row.get("pts_q1_last10_list"))
        if key == "rebounds":
            return coerce_numeric_list(row.get("reb_q1_last10_list"))
        if key == "assists":
            return coerce_numeric_list(row.get("ast_q1_last10_list"))
        if key == "steals":
            return coerce_numeric_list(row.get("stl_q1_last10_list"))
        if key == "blocks":
            return coerce_numeric_list(row.get("blk_q1_last10_list"))
        if key == "pra":
            return coerce_numeric_list(row.get("pra_q1_last10_list"))
        if key == "points_rebounds":
            return coerce_numeric_list(row.get("pr_q1_last10_list"))
        if key == "points_assists":
            return coerce_numeric_list(row.get("pa_q1_last10_list"))
        if key == "rebounds_assists":
            return coerce_numeric_list(row.get("ra_q1_last10_list"))
        return []

    # -----------------------
    # FULL GAME (EXISTING)
    # -----------------------
    if key == "points":
        return coerce_numeric_list(row.get("pts_last10_list"))
    if key == "rebounds":
        return coerce_numeric_list(row.get("reb_last10_list"))
    if key == "assists":
        return coerce_numeric_list(row.get("ast_last10_list"))
    if key == "steals":
        return coerce_numeric_list(row.get("stl_last10_list"))
    if key == "blocks":
        return coerce_numeric_list(row.get("blk_last10_list"))
    if key == "pra":
        return coerce_numeric_list(row.get("pra_last10_list"))
    if key == "points_rebounds":
        return coerce_numeric_list(row.get("pr_last10_list"))
    if key == "points_assists":
        return coerce_numeric_list(row.get("pa_last10_list"))
    if key == "rebounds_assists":
        return coerce_numeric_list(row.get("ra_last10_list"))

    return []

    
def pretty_market_label(market: str) -> str:
    m = (market or "").lower()

    if "points_rebounds_assists" in m:
        return "PRA"
    if "points_assists" in m:
        return "Pts + Ast"
    if "points_rebounds" in m:
        return "Pts + Reb"
    if "rebounds_assists" in m:
        return "Reb + Ast"
    if "points" in m:
        return "Points"
    if "rebounds" in m:
        return "Rebounds"
    if "assists" in m:
        return "Assists"

    return (
        m.replace("player_", "")
         .replace("_alternate", "")
         .replace("_", " ")
         .title()
    )


def build_l10_sparkline_html(values, line_value, dates=None):
    if not values or line_value is None:
        return ""

    try:
        vals = [float(v) for v in values if isinstance(v, (int, float))]
        if not vals:
            return ""

        line_f = float(line_value)

        # same bar scale you already use
        bar_min = 14
        bar_span = 26
        chart_h = bar_min + bar_span  # 40px

        vmin = min(min(vals), line_f)
        vmax = max(max(vals), line_f)
        span = max(vmax - vmin, 1.0)
    except Exception:
        return ""

    bars_html = []
    dates_html = []

    for i, v in enumerate(vals):
        pct = (v - vmin) / span
        height = int(bar_min + bar_span * pct)
        color = "#22c55e" if v >= line_f else "#ef4444"
        value_label = f"{v:.0f}"

        date_label = ""
        if dates is not None and len(dates) > i:
            try:
                date_label = pd.to_datetime(dates[i]).strftime("%m/%d")
            except Exception:
                date_label = str(dates[i])

        # BAR SLOT (fixed chart height; value label absolute; bar absolute)
        bars_html.append(
            f"<div style='"
            f"position:relative;"
            f"height:{chart_h}px;"
            f"width:10px;"
            f"display:flex;"
            f"justify-content:center;"
            f"'>"
            f"<div style='"
            f"position:absolute;"
            f"left:50%;"
            f"transform:translateX(-50%);"
            f"bottom:{min(height + 2, chart_h - 1)}px;"
            f"font-size:9px;"
            f"opacity:0.85;"
            f"line-height:1;"
            f"white-space:nowrap;"
            f"'>"
            f"{value_label}"
            f"</div>"
            f"<div style='"
            f"position:absolute;"
            f"left:50%;"
            f"transform:translateX(-50%);"
            f"bottom:0;"
            f"width:6px;"
            f"height:{height}px;"
            f"background:{color};"
            f"border-radius:2px;"
            f"'></div>"
            f"</div>"
        )

        # DATE SLOT (separate row so it doesn't mess with prop-line baseline)
        dates_html.append(
            f"<div style='"
            f"width:10px;"
            f"display:flex;"
            f"justify-content:center;"
            f"margin-top:3px;"
            f"font-size:9px;"
            f"opacity:0.6;"
            f"writing-mode:vertical-rl;"
            f"text-orientation:mixed;"
            f"line-height:1;"
            f"'>"
            f"{date_label}"
            f"</div>"
        )

    # prop line inside chart area (measured from bar baseline)
    line_pct = (line_f - vmin) / span
    line_y = int(chart_h * line_pct)

    return (
        f"<div style='display:flex;flex-direction:column;align-items:center;'>"
        f"<div style='"
        f"position:relative;"
        f"display:flex;"
        f"align-items:flex-end;"
        f"gap:4px;"
        f"margin-top:8px;"
        f"height:{chart_h}px;"
        f"'>"
        f"<div style='"
        f"position:absolute;"
        f"left:0;"
        f"right:0;"
        f"bottom:{line_y}px;"
        f"height:1px;"
        f"background:rgba(255,255,255,0.35);"
        f"'></div>"
        f"{''.join(bars_html)}"
        f"</div>"
        f"<div style='display:flex;gap:4px;align-items:flex-start;'>"
        f"{''.join(dates_html)}"
        f"</div>"
        f"</div>"
    )


@st.cache_data(show_spinner=False)
def build_prop_cards(card_df: pd.DataFrame, hit_rate_col: str) -> pd.DataFrame:
    """
    Dedupe identical props across books and attach a compact list of book prices.
    This keeps render loops smaller and avoids repeated cards.
    """
    if card_df.empty:
        return card_df

    # Use only columns that actually exist
    key_cols = [
        c for c in
        ["player", "player_team", "opponent_team", "market", "line", "bet_type"]
        if c in card_df.columns
    ]

    work = card_df.copy()


    # Normalize bookmaker + price
    if "bookmaker" in work.columns:
        work["book_norm"] = work["bookmaker"].astype(str).str.strip()
    else:
        work["book_norm"] = ""

    work["price_int"] = pd.to_numeric(work.get("price"), errors="coerce").fillna(0).astype(int)

    # Pick best row per prop (highest hit rate, then best odds)
    base = (
        work.sort_values(by=[hit_rate_col, "price_int"], ascending=[False, True])
            .drop_duplicates(key_cols, keep="first")
            .copy()
    )

    # Compact book list
    rows = []
    for _, sub in work.groupby(key_cols, dropna=False, sort=False):
        book_prices = []
        seen = set()
        for b, p in zip(sub["book_norm"], sub["price_int"]):
            bp = (b, int(p))
            if bp in seen:
                continue
            seen.add(bp)
            book_prices.append({"book": b, "price": int(p)})
        rows.append((*[sub.iloc[0][c] for c in key_cols], book_prices))

    books_df = pd.DataFrame(rows, columns=key_cols + ["book_prices"])
    out = base.merge(books_df, on=key_cols, how="left")
    out["book_prices"] = out["book_prices"].apply(lambda x: x if isinstance(x, list) else [])
    out.flags.writeable = False
    return out

def render_prop_cards(
    df: pd.DataFrame,
    hit_rate_col: str,
    hit_label: str,
    *,
    market_window: str,
):
    if df.empty:
        st.info(f"No props match your filters.")
        return

    if hit_rate_col not in df.columns:
        st.warning(f"Missing column: {hit_rate_col}")
        return

    card_df = build_prop_cards(df, hit_rate_col=hit_rate_col)

    for _, row in card_df.iterrows():

        player = f"{row.get('player', '')}"
        raw_market = row.get("market")
        norm = normalize_market_key(raw_market)
        base_label = pretty_market_label(raw_market)

        if market_window == "Q1":
            market_label = f"{base_label} 1st Quarter"
        else:
            market_label = base_label
        bet_type = f"{row.get('bet_type', '')}"

        team = f"{row.get('player_team', '')}"
        home_team = row.get("home_team")
        away_team = row.get("away_team")

        home_team = home_team.strip().upper() if isinstance(home_team, str) else None
        away_team = away_team.strip().upper() if isinstance(away_team, str) else None



        opp = f"{row.get('opponent_team', '')}"
        line = row.get("line")
        odds = row.get("price")

        bookmaker = f"{row.get('bookmaker', '')}"
        book_logo = SPORTSBOOK_LOGOS.get(bookmaker, "")

        # -----------------------------
        # TEAM LOGOS
        # -----------------------------
        home_logo = safe_team_logo(home_team)
        away_logo = safe_team_logo(away_team)



        hit = row.get(hit_rate_col)
        implied = row.get("implied_prob")

        if implied is None or pd.isna(implied):
            implied = compute_implied_prob(odds)

        edge = None
        if hit is not None and implied is not None and not pd.isna(hit) and not pd.isna(implied):
            edge = float(hit) - float(implied)

        books = row.get("book_prices", [])
        books_line = f" • ".join(
            f"{b.get('book','')} {fmt_odds(b.get('price'))}"
            for b in books[:4]
        )

        # -----------------------------
        # L10 SPARKLINE
        # -----------------------------
        l10_values = get_l10_values(
            row,
            market_window=market_window,
        )
        
        if not l10_values:
            st.caption(
                f"⚠️ No L10 values for {player} | market={raw_market} | window={market_window}"
            )

        # -----------------------------
        # STAT-SPECIFIC ROLLING AVERAGES
        # -----------------------------
        stat_key = normalize_market_key(raw_market)
        
        l5_avg  = row.get("avg_stat_l5")
        l10_avg = row.get("avg_stat_l10")
        l20_avg = row.get("avg_stat_l20")
        
        # -----------------------------
        # OPPONENT POSITIONAL RANK
        # -----------------------------
        opp_rank_map = {
            "points": "opp_pos_pts_rank",
            "rebounds": "opp_pos_reb_rank",
            "assists": "opp_pos_ast_rank",
            "steals": "opp_pos_stl_rank",
            "blocks": "opp_pos_blk_rank",
            "pra": "opp_pos_pra_rank",
            "points_rebounds": "opp_pos_pr_rank",
            "points_assists": "opp_pos_pa_rank",
            "rebounds_assists": "opp_pos_ra_rank",
        }
        
        opp_rank_col = opp_rank_map.get(stat_key)
        opp_rank = row.get(opp_rank_col) if opp_rank_col else None
        
        # -----------------------------
        # CONFIDENCE SCORE
        # -----------------------------
        confidence, confidence_parts = compute_confidence(
            row,
            hit_rate_col=hit_rate_col,
            stat_key=stat_key,
        )
                
        # -----------------------------
        # L10 SPARKLINE
        # -----------------------------
        dates = (
            row.get("last10_q1_dates")
            if market_window == "Q1"
            else row.get("last10_dates")
        )
        
        spark_html = build_l10_sparkline_html(
            values=l10_values,
            line_value=line,
            dates=dates,
        )


        # --------------------------------------------------
        # BASE CARD HTML (STRICT f-STRINGS)
        # --------------------------------------------------
        base_card_html = (
            f"<div class='prop-card card-grid'>"
        
            # ==================================================
            # TOP BAR: MATCHUP | PLAYER + MARKET | BOOK + ODDS
            # ==================================================
            f"<div style='display:grid;grid-template-columns:1fr 2fr 1fr;align-items:center;'>"
        
            # ---------- LEFT: MATCHUP ----------
            f"<div style='display:flex;align-items:center;gap:8px;font-size:0.8rem;opacity:0.9;'>"
            f"<img src='{away_logo}' style='width:22px;height:22px;' />"
            f"<span style='font-weight:700;'>@</span>"
            f"<img src='{home_logo}' style='width:22px;height:22px;' />"
            f"</div>"

        
            # ---------- CENTER: PLAYER + MARKET ----------
            f"<div style='text-align:center;'>"
            f"<div style='font-weight:900;font-size:1.15rem;letter-spacing:-0.2px;'>"
            f"{player}"
            f"</div>"
            f"<div style='font-size:0.85rem;opacity:0.7;'>"
            f"{market_label} – {bet_type.upper()} {fmt_num(line, 1)}"
            f"</div>"
            f"</div>"
        
            # ---------- RIGHT: BOOK + ODDS ----------
            f"<div style='display:flex;justify-content:flex-end;align-items:center;gap:8px;'>"
            f"<img src='{book_logo}' style='height:16px;width:auto;' />"
            f"<strong style='font-size:0.9rem;'>{fmt_odds(odds)}</strong>"
            f"</div>"
        
            f"</div>"
        
            # ==================================================
            # BOTTOM STATS ROW (L10 | OPP RANK | CONFIDENCE)
            # ==================================================
            f"<div style='display:grid;"
            f"grid-template-columns:1fr 1fr 1fr;"
            f"font-size:0.75rem;opacity:0.85;margin-top:6px;'>"
            
            # ---------- LEFT: L10 HIT + AVG ----------
            f"<div>"
            f"<strong>{fmt_pct(hit)}</strong>"
            f" <span style='opacity:0.5'>|</span> "
            f"<strong>{fmt_num(l10_avg, 1)}</strong><br/>"
            f"<span style='opacity:0.6'>L10 Hit | Avg</span>"
            f"</div>"
            
            # ---------- CENTER: OPP RANK ----------
            f"<div style='text-align:center;'>"
            f"<strong>{opp_rank if opp_rank is not None else '—'}</strong><br/>"
            f"<span style='opacity:0.6'>Opp Rank</span>"
            f"</div>"
            
            # ---------- RIGHT: CONFIDENCE ----------
            f"<div style='text-align:right;'>"
            f"<strong>{confidence}</strong><br/>"
            f"<span style='opacity:0.6'>Confidence</span>"
            f"</div>"
            
            f"</div>"
        
            f"</div>"
        )

        # --------------------------------------------------
        # EXPANDED HTML (UNCHANGED)
        # --------------------------------------------------
        expanded_html = (
            f"<div class='expanded-wrap'>"
        
            # ==================================================
            # SPARKLINE (MOVED HERE)
            # ==================================================
            f"<div style='display:flex;justify-content:center;margin-bottom:10px;'>"
            f"{build_l10_sparkline_html(values=l10_values, line_value=line, dates=dates)}"
            f"</div>"
        
            # ==================================================
            # ROW 1 — AVERAGES
            # ==================================================
            f"<div class='expanded-row'>"
            f"<div class='metric'><span>L5</span><strong>{fmt_num(l5_avg, 1)}</strong></div>"
            f"<div class='metric'><span>L10</span><strong>{fmt_num(l10_avg, 1)}</strong></div>"
            f"<div class='metric'><span>L20</span><strong>{fmt_num(l20_avg, 1)}</strong></div>"
            f"<div class='metric'><span>Δ Line</span>"
            f"<strong>{fmt_num(row.get('proj_diff_vs_line'), 1)}</strong>"
            f"</div>"
            f"</div>"
        
            # ==================================================
            # ROW 2 — L20 DISTRIBUTION
            # ==================================================
            f"<div class='expanded-row dist-row'>"
            f"<div class='metric'><span>L20 Hit</span><strong>{fmt_pct(row.get('dist20_hit_rate'))}</strong></div>"
            f"<div class='metric'><span>+1</span><strong>{fmt_pct(row.get('dist20_clear_1p_rate'))}</strong></div>"
            f"<div class='metric'><span>+2</span><strong>{fmt_pct(row.get('dist20_clear_2p_rate'))}</strong></div>"
            f"<div class='metric'><span>Bad</span><strong>{fmt_pct(row.get('dist20_fail_bad_rate'))}</strong></div>"
            f"<div class='metric'><span>Margin</span><strong>{fmt_num(row.get('dist20_avg_margin'), 1)}</strong></div>"
            f"</div>"
        
            # ==================================================
            # ROW 3 — L40 DISTRIBUTION
            # ==================================================
            f"<div class='expanded-row dist-row'>"
            f"<div class='metric'><span>L40 Hit</span><strong>{fmt_pct(row.get('dist40_hit_rate'))}</strong></div>"
            f"<div class='metric'><span>+1</span><strong>{fmt_pct(row.get('dist40_clear_1p_rate'))}</strong></div>"
            f"<div class='metric'><span>+2</span><strong>{fmt_pct(row.get('dist40_clear_2p_rate'))}</strong></div>"
            f"<div class='metric'><span>Bad</span><strong>{fmt_pct(row.get('dist40_fail_bad_rate'))}</strong></div>"
            f"<div class='metric'><span>Margin</span><strong>{fmt_num(row.get('dist40_avg_margin'), 1)}</strong></div>"
            f"</div>"
        
            # ==================================================
            # ROW 4 — WOWY / INJURY (SAFE PLACEHOLDER)
            # ==================================================
            f"<div class='expanded-row wowy-row'>"
            f"<div class='metric' style='flex:1;opacity:0.6;'>"
            f"Injury / WOWY data coming soon"
            f"</div>"
            f"</div>"
        
            f"</div>"
        )

        # -------------------------
        # SAVE BET (MINIMAL MEMORY)
        # -------------------------
        line_str = fmt_num(line, 1)
        odds_str = fmt_odds(odds)
        
        bet_line = (
            f"{player} | "
            f"{pretty_market_label(raw_market)} | "
            f"{line_str} | "
            f"{odds_str} | "
            f"{bet_type}"
        )
        
        save_key = (
            f"save_"
            f"{player}_"
            f"{raw_market}_"
            f"{line}_"
            f"{bet_type}_"
            f"page{st.session_state.page}_"
            f"idx{_}"
        )
        
        st.button(
            "💾 Save Bet",
            key=save_key,
            on_click=handle_save_bet,
            args=(bet_line,),
        )
        
        # Optional instant visual confirmation
        if bet_line in st.session_state.saved_bets_text:
            st.caption("✅ Saved")


        # -------------------------
        # CARD EXPAND UI
        # -------------------------
        st.markdown(
            f"<details class='prop-card-wrapper'>"
            f"<summary>"
            f"{base_card_html}"
            f"<div class='expand-hint'>Click to expand ▾</div>"
            f"</summary>"
            f"<div class='card-expanded'>"
            f"{expanded_html}"
            f"</div>"
            f"</details>",
            unsafe_allow_html=True,
        )

def build_first_basket_expanded_html(row: pd.Series) -> str:
    starter_pct = row.get("starter_pct")
    first_shot_share = row.get("first_shot_share")
    usage_l10 = row.get("usage_l10")
    pts_per_min = row.get("pts_per_min")

    team_first_score_rate = row.get("team_first_score_rate")
    tip_win_pct = row.get("tip_win_pct")

    return f"""
    <div class="card-expanded">

      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:12px;">
        <div>
          <div class="metric-label">Starter %</div>
          <div class="metric-value">{fmt_pct(starter_pct)}</div>
        </div>
        <div>
          <div class="metric-label">First Shot Share</div>
          <div class="metric-value">{fmt_pct(first_shot_share)}</div>
        </div>
        <div>
          <div class="metric-label">Usage (L10)</div>
          <div class="metric-value">{fmt_pct(usage_l10)}</div>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;">
        <div>
          <div class="metric-label">PTS / Min</div>
          <div class="metric-value">{fmt_num(pts_per_min, 2)}</div>
        </div>
        <div>
          <div class="metric-label">Team First Score</div>
          <div class="metric-value">{fmt_pct(team_first_score_rate)}</div>
        </div>
        <div>
          <div class="metric-label">Tip Win %</div>
          <div class="metric-value">{fmt_pct(tip_win_pct)}</div>
        </div>
      </div>

    </div>
    """

def render_first_basket_card(row: pd.Series):
    """
    Renders a single First Basket PLAYER card
    """

    player = row.get("player")
    team = row.get("team_abbr")

    prob = row.get("first_basket_probability")
    rank_game = row.get("rank_within_game")

    # logo
    team_logo = safe_team_logo(team)

    # -----------------------------
    # LEFT: TEAM LOGO
    # -----------------------------
    left_html = (
        f"<div style='display:flex;align-items:center;'>"
        f"<img src='{team_logo}' width='26' />"
        f"</div>"
    )

    # -----------------------------
    # CENTER: PLAYER + LABEL
    # -----------------------------
    title_html = (
        f"<div style='text-align:center;'>"
        f"<div style='font-weight:800;font-size:1.1rem;'>"
        f"{player}"
        f"</div>"
        f"<div style='opacity:0.6;font-size:0.8rem;'>"
        f"First Basket"
        f"</div>"
        f"</div>"
    )

    # -----------------------------
    # RIGHT: PROBABILITY + RANK
    # -----------------------------
    right_html = (
        f"<div style='text-align:right;'>"
        f"<div style='font-size:1.15rem;font-weight:900;'>"
        f"{fmt_pct(prob)}"
        f"</div>"
        f"<div style='opacity:0.6;font-size:0.7rem;'>"
        f"#{rank_game} in game"
        f"</div>"
        f"</div>"
    )

    # -----------------------------
    # BASE CARD
    # -----------------------------
    base_card_html = (
        f"<div class='prop-card card-grid'>"
        f"<div style='display:grid;"
        f"grid-template-columns:48px 1fr 80px;"
        f"align-items:center;'>"
        f"{left_html}"
        f"{title_html}"
        f"{right_html}"
        f"</div>"
        f"</div>"
    )

    expanded_html = build_first_basket_expanded_html(row)

    st.markdown(
        f"""
        <details class="prop-card-wrapper">
          <summary>
            {base_card_html}
            <div class="expand-hint">Click to expand ▾</div>
          </summary>
    
          {expanded_html}
    
        </details>
        """,
        unsafe_allow_html=True,
    )
    
def render_first_basket_cards(df: pd.DataFrame):
    """
    Renders First Basket cards grouped by game
    """

    if df.empty:
        return

    # Ensure clean ordering
    df = df.sort_values(["game_id", "rank_within_game"])

    for game_id, game_df in df.groupby("game_id"):

        # -----------------------------
        # GAME MATCHUP HEADER
        # -----------------------------
        home = game_df["home_team_abbr"].iloc[0]
        away = game_df["away_team_abbr"].iloc[0]

        home_logo = safe_team_logo(home)
        away_logo = safe_team_logo(away)

        st.markdown(
            f"""
            <div style="
                display:flex;
                align-items:center;
                gap:10px;
                margin:18px 6px 8px;
                font-weight:800;
                font-size:1.0rem;
                opacity:0.95;
            ">
                <img src="{away_logo}" width="22"/>
                <span>@</span>
                <img src="{home_logo}" width="22"/>
                <span>{away} @ {home}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # -----------------------------
        # OPTIONAL GAME CONTEXT ROW
        # -----------------------------
        team_rate = game_df["team_first_score_rate"].iloc[0]
        tip_pct = game_df["tip_win_pct"].iloc[0]

        st.markdown(
            f"""
            <div style="
                margin-left:40px;
                margin-bottom:10px;
                font-size:0.75rem;
                opacity:0.65;
            ">
                Team First Score: <strong>{fmt_pct(team_rate)}</strong>
                &nbsp;•&nbsp;
                Tip Win Rate: <strong>{fmt_pct(tip_pct)}</strong>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # -----------------------------
        # PLAYER CARDS (TOP N)
        # -----------------------------
        game_df = (
            game_df
            .sort_values("rank_within_game", ascending=True)
        )
        
        for _, row in game_df.iterrows():
            render_first_basket_card(row)

def render_lineup_player_row(row):
    player = row.get("player") or "—"
    slot = row.get("lineup_slot")

    slot_html = f"#{int(slot)}" if pd.notna(slot) else ""

    return (
        f"<div class='lineup-player'>"
        f"<strong style='opacity:0.7;margin-right:6px;'>{slot_html}</strong>"
        f"{player}"
        f"</div>"
    )

def render_most_used_lineup_card(team_df: pd.DataFrame):
    team_df = team_df.sort_values("lineup_slot")

    players_html = "".join(
        render_lineup_player_row(row)
        for _, row in team_df.iterrows()
    )

    times_used = team_df["times_used"].iloc[0]
    first_used = team_df["first_used"].iloc[0]
    last_used  = team_df["last_used"].iloc[0]

    base_card_html = (
        f"<div class='prop-card card-grid'>"
        f"<div class='prop-card-title'>Most Used Lineup</div>"
        f"<div class='lineup-list'>{players_html}</div>"
        f"<div class='lineup-subtitle'>Used {times_used} times</div>"
        f"</div>"
    )

    expanded_html = (
        f"<div class='expanded-wrap'>"
        f"<div class='expanded-row'>"
        f"<div class='metric'><span>First Used</span><strong>{first_used}</strong></div>"
        f"<div class='metric'><span>Last Used</span><strong>{last_used}</strong></div>"
        f"</div>"
        f"</div>"
    )

    return (
        f"<details class='prop-card-wrapper'>"
        f"<summary>{base_card_html}<div class='expand-hint'>Click to expand ▾</div></summary>"
        f"<div class='card-expanded'>{expanded_html}</div>"
        f"</details>"
    )

def render_projected_lineup_card(team_df: pd.DataFrame):
    team_df = team_df.sort_values("lineup_slot")

    players_html = "".join(
        render_lineup_player_row(row)
        for _, row in team_df.iterrows()
    )

    projection_reason = team_df["projection_reason"].iloc[0]
    projected_at = team_df["projected_at"].iloc[0]
    projected_at_str = (
        pd.to_datetime(projected_at).strftime("%b %d · %I:%M %p ET")
        if pd.notna(projected_at)
        else "—"
    )


    base_card_html = (
        f"<div class='prop-card card-grid'>"
        f"<div class='prop-card-title'>Projected Lineup</div>"
        f"<div class='lineup-list'>{players_html}</div>"
        f"<div class='lineup-subtitle'>{projection_reason}</div>"
        f"</div>"
    )

    expanded_html = (
        f"<div class='expanded-wrap'>"
        f"<div class='expanded-row'>"
        f"<div class='metric'><span>Projected At</span><strong>{projected_at_str}</strong></div>"
        f"</div>"
        f"</div>"
    )

    return (
        f"<details class='prop-card-wrapper'>"
        f"<summary>{base_card_html}<div class='expand-hint'>Click to expand ▾</div></summary>"
        f"<div class='card-expanded'>{expanded_html}</div>"
        f"</details>"
    )

def render_matchup_header(row):
    home = row["home_team_abbr"]
    away = row["away_team_abbr"]

    home_logo = safe_team_logo(home)
    away_logo = safe_team_logo(away)

    home_name = team_full_name(home)
    away_name = team_full_name(away)

    tip = row.get("start_time_est")
    tip_str = (
        pd.to_datetime(tip).strftime("%-I:%M %p ET")
        if tip is not None and not pd.isna(tip)
        else ""
    )

    return (
        f"<div class='matchup-header'>"
        f"<div class='matchup-team'>"
        f"<img src='{away_logo}' />"
        f"<span>{away_name}</span>"
        f"</div>"
        f"<div class='matchup-at'>@</div>"
        f"<div class='matchup-team'>"
        f"<img src='{home_logo}' />"
        f"<span>{home_name}</span>"
        f"</div>"
        f"<div class='matchup-time'>{tip_str}</div>"
        f"</div>"
    )


def render_team_header(team_abbr: str):
    logo_url = safe_team_logo(team_abbr)
    team_name = team_full_name(team_abbr)

    return (
        f"<div class='team-header-card'>"
        f"<img src='{logo_url}' />"
        f"<div class='team-header-name'>{team_name}</div>"
        f"</div>"
    )


def render_lineups_tab():
    # --------------------------------------------------
    # Load today's games
    # --------------------------------------------------
    games_df = load_todays_games()

    if games_df.empty:
        st.warning("No games today.")
        return

    # --------------------------------------------------
    # Determine teams playing today
    # --------------------------------------------------
    teams_today = sorted(
        set(games_df["home_team_abbr"]).union(games_df["away_team_abbr"])
    )

    # --------------------------------------------------
    # Load lineup data (filtered to teams playing today)
    # --------------------------------------------------
    most_used_df = load_team_most_used_lineups_for_teams(teams_today)
    projected_df = load_projected_starting_lineups_for_teams(teams_today)

    # --------------------------------------------------
    # Render by matchup
    # --------------------------------------------------
    for _, game in games_df.iterrows():
        away = game["away_team_abbr"]
        home = game["home_team_abbr"]

        # -----------------------------
        # Matchup Header
        # -----------------------------
        st.markdown(
            render_matchup_header(game),
            unsafe_allow_html=True,
        )


        # -----------------------------
        # Render both teams
        # -----------------------------
        for team_abbr in (away, home):

            # Team header (logo + name)
            st.markdown(
                render_team_header(team_abbr),
                unsafe_allow_html=True,
            )

            col1, col2 = st.columns(2)

            team_most_used = most_used_df[
                most_used_df["team_abbr"] == team_abbr
            ]

            team_projected = projected_df[
                projected_df["team_abbr"] == team_abbr
            ]

            # -------------------------
            # Most Used Lineup
            # -------------------------
            with col1:
                if team_most_used.empty:
                    st.info("No historical lineup available")
                else:
                    st.markdown(
                        render_most_used_lineup_card(team_most_used),
                        unsafe_allow_html=True,
                    )

            # -------------------------
            # Projected Lineup
            # -------------------------
            with col2:
                if team_projected.empty:
                    st.info("No projected lineup available")
                else:
                    st.markdown(
                        render_projected_lineup_card(team_projected),
                        unsafe_allow_html=True,
                    )

        # Divider between matchups
        st.markdown("<hr/>", unsafe_allow_html=True)

# ------------------------------------------------------
# MAIN APP
# ------------------------------------------------------
st.title("Pulse Sports Analytics — Minimal Core")

# Sidebar: Dev Tools link (no heavy work)
if IS_DEV and is_dev_user():
    st.sidebar.divider()
    st.sidebar.markdown("### ⚙️ Dev Tools")
    if st.sidebar.button("Open DEV Tools"):
        st.query_params["tab"] = "dev"
        st.rerun()

st.sidebar.divider()
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Tabs: Props + Lineups + First Basket + Saved Bets
tab_props, tab_lineups, tab_first_basket, tab_saved = st.tabs(
    ["📈 Props", "🧩 Lineups", "🥇 First Basket", "📋 Saved Bets"]
)

with tab_saved:
    render_saved_bets()
    
with tab_first_basket:
    render_first_basket_tab()
    
with tab_lineups:
    render_lineups_tab()

with tab_props:
    # --------------------------------------------------
    # MARKET WINDOW (FULL / Q1)
    # --------------------------------------------------
    market_window = st.radio(
        "Market Window",
        ["FULL", "Q1"],
        horizontal=True,
    )

    PROPS_TABLE = (
        "props_full_enriched"
        if market_window == "FULL"
        else "props_q1_enriched"
    )

    # --------------------------------------------------
    # LOAD PROPS
    # --------------------------------------------------
    props_df = load_props(PROPS_TABLE)
    record_memory_checkpoint()

    if props_df.empty:
        st.info("No props returned from BigQuery.")
        st.stop()

    # --------------------------------------------------
    # LOAD PLAYER TRENDS (1 ROW PER PLAYER)
    # --------------------------------------------------
    if market_window == "Q1":
        trends_df = load_trends_q1()
    else:
        trends_df = load_trends()

    if not trends_df["player"].is_unique:
        st.error("❌ Trends table must be 1 row per player (merge aborted)")
        st.stop()

    # --------------------------------------------------
    # MERGE TRENDS → PROPS (SAFE)
    # --------------------------------------------------
    props_df = props_df.merge(
        trends_df,
        on="player",
        how="left",
        validate="many_to_one",
    )

    props_df.flags.writeable = False
    record_memory_checkpoint()

    # --------------------------------------------------
    # BUILD FILTER OPTIONS
    # --------------------------------------------------
    book_list = (
        sorted(props_df["bookmaker"].dropna().unique().tolist())
        if "bookmaker" in props_df.columns
        else []
    )

    games_today = []
    if "home_team" in props_df.columns and "visitor_team" in props_df.columns:
        games_today = sorted(
            (props_df["home_team"].astype(str) + " vs " + props_df["visitor_team"].astype(str))
            .dropna()
            .unique()
            .tolist()
        )

    # --------------------------------------------------
    # FILTER UI
    # --------------------------------------------------
    with st.expander("⚙️ Filters", expanded=False):

        c1, c2 = st.columns([1.2, 1.8])

        with c1:
            f_bet_type = st.multiselect(
                "Bet Type",
                ["Over", "Under"],
                default=["Over", "Under"],
            )

        MARKET_GROUPS = {
            "Points": ["player_points"],
            "Rebounds": ["player_rebounds"],
            "Assists": ["player_assists"],
            "Steals": ["player_steals"],
            "Blocks": ["player_blocks"],
            "Combos": [
                "player_pra",
                "player_pr",
                "player_pa",
                "player_ra",
            ],
            "Milestones": [
                "player_double_double",
                "player_triple_double",
            ],
        }

        with c2:
            selected_market_groups = st.multiselect(
                "Markets",
                list(MARKET_GROUPS.keys()),
                default=list(MARKET_GROUPS.keys()),
            )

        f_market = [
            m for g in selected_market_groups for m in MARKET_GROUPS[g]
        ]

        c3, c4 = st.columns([2, 1])

        with c3:
            f_min_odds, f_max_odds = st.slider(
                "Odds Range",
                -1000,
                1000,
                (-600, 150),
                step=25,
            )

        with c4:
            f_window = st.selectbox(
                "Hit Window",
                ["L5", "L10", "L20"],
                index=1,
            )

        default_books = [
            b for b in book_list
            if b.lower() in ("draftkings", "fanduel")
        ] or book_list

        f_books = st.multiselect(
            "Books",
            book_list,
            default=default_books,
        )

        show_games = st.checkbox("Filter by Games", value=False)

        if show_games:
            f_games = st.multiselect(
                "Games",
                games_today,
                default=games_today,
            )
        else:
            f_games = []

        st.divider()
        st.markdown("**Advanced Filters**")

        show_ev_only = st.checkbox(
            "Show only EV+ bets (Hit Rate > Implied Probability)",
            value=False,
        )

        f_min_hit = st.slider(
            "Min Hit Rate (%)",
            0,
            100,
            80,
        )

    # --------------------------------------------------
    # MEMORY WIDGET
    # --------------------------------------------------
    mem_now, mem_delta = finalize_render_memory()
    delta_icon = "🔴" if mem_delta > 5 else "🟢"

    st.caption(
        f"🧠 RAM: **{mem_now:.0f} MB** "
        f"{delta_icon} {mem_delta:+.1f} MB • "
        f"Render Peak: **{st.session_state.mem_render_peak_mb:.0f} MB** • "
        f"Session Peak: **{st.session_state.mem_peak_mb:.0f} MB**"
    )

    # --------------------------------------------------
    # APPLY FILTERS
    # --------------------------------------------------
    df = props_df.copy()

    if "bet_type" in df.columns:
        df["bet_type"] = (
            df["bet_type"]
            .astype(str)
            .str.strip()
            .str.lower()
            .replace({
                "count": "Over",
                "binary": "Over",
                "yes": "Over",
                "over": "Over",
                "under": "Under",
            })
        )

    if "bet_type" in df.columns:
        df = df[df["bet_type"].isin(f_bet_type)]

    if "market" in df.columns and f_market:
        df = df[df["market"].isin(f_market)]

    if "bookmaker" in df.columns and f_books:
        df = df[df["bookmaker"].isin(f_books)]

    if "price" in df.columns:
        df = df[(df["price"] >= f_min_odds) & (df["price"] <= f_max_odds)]

    if show_games and f_games and "home_team" in df.columns and "visitor_team" in df.columns:
        game_display = df["home_team"].astype(str) + " vs " + df["visitor_team"].astype(str)
        df = df[game_display.isin(f_games)]

    window_col = {
        "L5": "hit_rate_last5",
        "L10": "hit_rate_last10",
        "L20": "hit_rate_last20",
    }[f_window]

    hit_rate_decimal = f_min_hit / 100.0
    if window_col in df.columns:
        df = df[df[window_col] >= hit_rate_decimal]

    if show_ev_only and window_col in df.columns:
        implied = df["implied_prob"].fillna(
            df["price"].apply(compute_implied_prob)
        )
        df = df[df[window_col] > implied]

    if window_col in df.columns and "price" in df.columns:
        df = df.sort_values([window_col, "price"], ascending=[False, True])

    # --------------------------------------------------
    # PAGINATION
    # --------------------------------------------------
    PAGE_SIZE = 30

    if "page" not in st.session_state:
        st.session_state.page = 0

    page_key = (
        f"{len(df)}|{window_col}|"
        f"{','.join(sorted(f_market))}|"
        f"{','.join(sorted(f_books))}|"
        f"{','.join(sorted(f_games))}|"
        f"{show_ev_only}"
    )

    if st.session_state.get("_last_page_key") != page_key:
        st.session_state.page = 0
        st.session_state._last_page_key = page_key

    total_rows = len(df)
    total_pages = max(1, math.ceil(total_rows / PAGE_SIZE))

    start = st.session_state.page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_df = df.iloc[start:end]

    col_prev, col_mid, col_next = st.columns([1, 2, 1])

    with col_prev:
        if st.button("⬅ Prev", disabled=st.session_state.page == 0):
            st.session_state.page -= 1

    with col_next:
        if st.button("Next ➡", disabled=st.session_state.page >= total_pages - 1):
            st.session_state.page += 1

    with col_mid:
        st.caption(
            f"Page {st.session_state.page + 1} of {total_pages} "
            f"({total_rows} results)"
        )

    # --------------------------------------------------
    # RENDER CARDS
    # --------------------------------------------------
    render_prop_cards(
        df=page_df,
        hit_rate_col=window_col,
        hit_label=f_window,
        market_window=market_window,
    )

    record_memory_checkpoint()

