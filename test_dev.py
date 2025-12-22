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
# DATA: PROPS (ONLY TABLE WE LOAD)
# ------------------------------------------------------
PROPS_SQL = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{PROPS_TABLE}`"

@st.cache_data(ttl=900, show_spinner=True)
def load_props() -> pd.DataFrame:
    df = load_bq_df(PROPS_SQL)

    # Keep only columns we actually use (cuts memory)
    keep = [
        "player", "player_team",
        "home_team", "visitor_team", "opponent_team",
        "market", "line", "bet_type",
        "bookmaker", "price",
        "hit_rate_last5", "hit_rate_last10", "hit_rate_last20",
        "implied_prob",
        "edge_pct", "edge_raw",
        "game_date",
    ]
    cols = [c for c in keep if c in df.columns]
    df = df[cols].copy()

    # Light normalization
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if "line" in df.columns:
        df["line"] = pd.to_numeric(df["line"], errors="coerce")

    for c in ("hit_rate_last5", "hit_rate_last10", "hit_rate_last20", "implied_prob", "edge_pct", "edge_raw"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    # Fill strings without expanding memory too much
    for c in ("player", "market", "bet_type", "bookmaker", "player_team", "home_team", "visitor_team", "opponent_team"):
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

def render_saved_bets():
    init_saved_bets_state()
    bets = st.session_state.saved_bets

    st.subheader("📋 Saved Bets")
    st.caption(f"Stored in session only • capped at {MAX_SAVED_BETS}")

    if not bets:
        st.info("No saved bets yet.")
        return

    # small rendering loop
    export_lines = []
    for b in bets:
        player = b.get("player", "")
        market = b.get("market", "")
        bet_type = b.get("bet_type", "")
        line = b.get("line", None)
        st.markdown(f"**{player}**  \n{market} **{bet_type} {line}**")
        st.divider()
        export_lines.append(f"{player} — {bet_type} {line} ({market})")

    st.text_area("Copy for Pikkit", "\n".join(export_lines), height=200)

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

@st.cache_data(show_spinner=False)
def build_prop_cards(card_df: pd.DataFrame, hit_rate_col: str) -> pd.DataFrame:
    """
    Dedupe identical props across books and attach a compact list of book prices.
    This keeps render loops smaller and avoids repeated cards.
    """
    if card_df.empty:
        return card_df

    key_cols = ["player", "player_team", "opponent_team", "market", "line", "bet_type"]
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

def render_prop_cards(df: pd.DataFrame, hit_rate_col: str, hit_label: str):
    if df.empty:
        st.info("No props match your filters.")
        return

    # ensure column exists
    if hit_rate_col not in df.columns:
        st.warning(f"Missing column: {hit_rate_col}")
        return

    # build compact card df
    card_df = build_prop_cards(df, hit_rate_col=hit_rate_col)

    st.markdown("<div class='prop-grid'>", unsafe_allow_html=True)

    # Render in a single column to keep DOM smaller (lower memory)
    for _, row in card_df.iterrows():
        player = row.get("player", "")
        market = row.get("market", "")
        bet_type = row.get("bet_type", "")
        line = row.get("line", None)
        team = row.get("player_team", "")
        opp = row.get("opponent_team", "")
        odds = row.get("price", None)

        hit = row.get(hit_rate_col, None)
        implied = row.get("implied_prob", None)
        if implied is None or (isinstance(implied, float) and pd.isna(implied)):
            implied = compute_implied_prob(odds)

        edge = None
        if hit is not None and implied is not None and not pd.isna(hit) and not pd.isna(implied):
            edge = float(hit) - float(implied)

        books = row.get("book_prices", [])
        books_line = " • ".join(f"{b.get('book','')} {fmt_odds(b.get('price'))}" for b in books[:4])  # cap

        base_card_html = (
            f"<div class='prop-card'>"
            f"<div style='display:flex;justify-content:space-between;gap:10px;'>"
            f"  <div style='font-weight:800;font-size:1.02rem;line-height:1.1'>{player}</div>"
            f"  <div style='opacity:0.75;font-size:0.85rem'>{team} vs {opp}</div>"
            f"</div>"
            f"<div style='margin-top:6px;display:flex;justify-content:space-between;gap:10px;'>"
            f"  <div style='font-weight:650'>{market}</div>"
            f"  <div style='opacity:0.85'>{bet_type} {fmt_num(line, 1)}</div>"
            f"</div>"
            f"<div style='margin-top:8px;display:flex;justify-content:space-between;gap:10px;'>"
            f"  <div style='opacity:0.85'>{hit_label}: <strong>{fmt_pct(hit)}</strong></div>"
            f"  <div style='opacity:0.85'>Odds: <strong>{fmt_odds(odds)}</strong></div>"
            f"</div>"
            f"<div style='margin-top:6px;opacity:0.75;font-size:0.82rem'>{books_line}</div>"
            f"</div>"
        )

        expanded_html = (
            f"<div class='expanded-wrap'>"
            f"  <div class='expanded-row'>"
            f"    <div class='metric'><span>Implied</span><strong>{fmt_pct(implied)}</strong></div>"
            f"    <div class='metric'><span>Edge</span><strong>{fmt_pct(edge) if edge is not None else '—'}</strong></div>"
            f"    <div class='metric'><span>Line</span><strong>{fmt_num(line, 1)}</strong></div>"
            f"  </div>"
            f"</div>"
        )

        # Save Bet (simple, constant memory)
        save_key = f"save_{player}_{market}_{line}_{bet_type}"
        if st.button("💾 Save Bet", key=save_key):
            ok = save_bet_simple(player=player, market=market, line=line, price=odds, bet_type=bet_type)
            st.toast("Saved ✅" if ok else "Already saved")

        # Card expand UI
        st.markdown(
            f"<details class='prop-card-wrapper'>"
            f"<summary>{base_card_html}<div class='expand-hint'>Click to expand ▾</div></summary>"
            f"<div class='card-expanded'>{expanded_html}</div>"
            f"</details>",
            unsafe_allow_html=True,
        )

    st.markdown("</div>", unsafe_allow_html=True)

# ------------------------------------------------------
# DEV TAB CONTENT (keep, but avoid heavy data pulls)
# ------------------------------------------------------
def trigger_apps_script(task: str):
    if not APPS_SCRIPT_URL or not APPS_SCRIPT_DEV_TOKEN:
        raise RuntimeError("Missing APPS_SCRIPT_URL or APPS_SCRIPT_DEV_TOKEN")

    resp = requests.post(
        APPS_SCRIPT_URL,
        headers={"Content-Type": "application/json"},
        params={"token": APPS_SCRIPT_DEV_TOKEN},
        json={"task": task},
        timeout=60,
    )
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(data.get("message") or "Apps Script error")
    return data.get("message") or "OK"

def render_dev_page():
    st.title("⚙️ DEV CONTROL PANEL (Minimal)")
    st.caption("Restricted • low-memory tools only")
    st.markdown(f"**Email:** `{get_user_email()}`")

    if st.button("⬅ Back to Main App"):
        nav_to("main")
        st.rerun()

    st.divider()
    st.subheader("📄 Google Apps Script")
    tasks = [
        ("NBA Alternate Props", "NBA_ALT_PROPS"),
        ("NBA Game Odds", "NBA_GAME_ODDS"),
        ("NCAAB Game Odds", "NCAAB_GAME_ODDS"),
        ("Run ALL (Daily Runner)", "ALL"),
    ]
    for label, task in tasks:
        c1, c2 = st.columns([3, 1])
        with c1:
            st.markdown(f"**{label}**")
        with c2:
            if st.button("▶ Run", key=f"apps_{task}", use_container_width=True):
                try:
                    with st.spinner(f"Running {label}…"):
                        msg = trigger_apps_script(task)
                    st.success(f"✅ {msg}")
                except Exception as e:
                    st.error("❌ Apps Script trigger failed")
                    st.code(str(e))

    st.divider()
    st.subheader("🔎 Quick Health Checks")
    if st.button("Test BigQuery connection"):
        try:
            _ = load_bq_df("SELECT 1 AS ok")
            st.success("✅ BigQuery OK")
        except Exception as e:
            st.error("❌ BigQuery failed")
            st.code(str(e))

# ------------------------------------------------------
# EARLY ROUTE: DEV TAB MUST NOT LOAD MAIN DATA
# ------------------------------------------------------
active_tab = get_active_tab()
if active_tab == "dev":
    if not is_dev_user():
        st.error("⛔ Access denied")
        st.stop()
    render_dev_page()
    st.stop()

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

# Tabs: Props + Saved Bets (only)
tab_props, tab_saved = st.tabs(["📈 Props", "📋 Saved Bets"])

with tab_saved:
    render_saved_bets()

with tab_props:
    props_df = load_props()

    if props_df.empty:
        st.info("No props returned from BigQuery.")
        st.stop()

    # Build filter options lightly
    market_list = sorted(props_df["market"].dropna().unique().tolist()) if "market" in props_df.columns else []
    book_list = sorted(props_df["bookmaker"].dropna().unique().tolist()) if "bookmaker" in props_df.columns else []
    games_today = []
    if "home_team" in props_df.columns and "visitor_team" in props_df.columns:
        games_today = sorted(
            (props_df["home_team"].astype(str) + " vs " + props_df["visitor_team"].astype(str)).dropna().unique().tolist()
        )

    # Collapsible filter panel (kept)
    with st.expander("⚙️ Filters", expanded=False):
        c1, c2, c3 = st.columns([1.2, 1.7, 1.5])
        with c1:
            f_bet_type = st.multiselect("Bet Type", options=["Over", "Under"], default=["Over", "Under"])
        with c2:
            f_market = st.multiselect("Market", options=market_list, default=market_list)
        with c3:
            f_games = st.multiselect("Games", options=games_today, default=games_today)

        c4, c5, c6 = st.columns([1, 1, 1])
        with c4:
            f_min_odds = st.number_input("Min Odds", value=-600, step=10)
        with c5:
            f_max_odds = st.number_input("Max Odds", value=150, step=10)
        with c6:
            f_window = st.selectbox("Hit Window", ["L5", "L10", "L20"], index=1)

        c7 = st.columns([1])[0]
        with c7:
            default_books = [b for b in book_list if b.lower() in ("draftkings", "fanduel")] or book_list
            f_books = st.multiselect("Books", options=book_list, default=default_books)

        show_ev_only = st.checkbox(
            "Show only EV+ bets (Hit Rate > Implied Probability)",
            value=False
        )

        f_min_hit = st.slider("Min Hit Rate (%)", 0, 100, 80)

    # Apply filters without copying big DF too much
    df = props_df
    if "bet_type" in df.columns:
        df = df[df["bet_type"].isin(f_bet_type)]
    if "market" in df.columns and f_market:
        df = df[df["market"].isin(f_market)]
    if "bookmaker" in df.columns and f_books:
        df = df[df["bookmaker"].isin(f_books)]
    if "price" in df.columns:
        df = df[(df["price"] >= f_min_odds) & (df["price"] <= f_max_odds)]
    if games_today and f_games and "home_team" in df.columns and "visitor_team" in df.columns:
        game_display = (df["home_team"].astype(str) + " vs " + df["visitor_team"].astype(str))
        df = df[game_display.isin(f_games)]

    window_col = {"L5": "hit_rate_last5", "L10": "hit_rate_last10", "L20": "hit_rate_last20"}[f_window]
    hit_rate_decimal = f_min_hit / 100.0
    if window_col in df.columns:
        df = df[df[window_col] >= hit_rate_decimal]

    if show_ev_only:
        # vectorized where possible
        if "implied_prob" in df.columns:
            implied = df["implied_prob"]
        else:
            implied = df["price"].apply(compute_implied_prob) if "price" in df.columns else None
        if implied is not None and window_col in df.columns:
            df = df[df[window_col] > implied]

    if window_col in df.columns and "price" in df.columns:
        df = df.sort_values([window_col, "price"], ascending=[False, True])

    render_prop_cards(df=df, hit_rate_col=window_col, hit_label=f_window)
