# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
import os
import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import pytz
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from urllib.parse import urlencode
import html as html_lib

# ------------------------------------------------------
# TIMEZONE (EST)
# ------------------------------------------------------
EST = pytz.timezone("America/New_York")

# ------------------------------------------------------
# STREAMLIT CONFIG
# ------------------------------------------------------
st.set_page_config(page_title="NBA Prop Analyzer", layout="wide")

# Global CSS to center table text
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
DATASET = "nba_prop_analyzer"
PROPS_TABLE = "todays_props_with_hit_rates"
HISTORICAL_TABLE = "historical_player_stats_for_trends"
SERVICE_JSON = os.getenv("GCP_SERVICE_ACCOUNT", "")

if not PROJECT_ID or not SERVICE_JSON:
    st.error("❌ Missing PROJECT_ID or GCP_SERVICE_ACCOUNT environment variables.")
    st.stop()

# ------------------------------------------------------
# SQL STATEMENTS
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
# AUTHENTICATION
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
    st.write("✅ Credentials loaded.")
except Exception as e:
    st.error(f"❌ Credential error: {e}")
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

if "active_tab" not in st.session_state:
    st.session_state.active_tab = "🧮 Props Overview"

if "trend_player" not in st.session_state:
    st.session_state.trend_player = None

if "trend_market" not in st.session_state:
    st.session_state.trend_market = None

if "trend_line" not in st.session_state:
    st.session_state.trend_line = None

if "processed_actions" not in st.session_state:
    st.session_state.processed_actions = set()

# ------------------------------------------------------
# QUERY PARAM HANDLING
# ------------------------------------------------------
params = st.query_params

action = params.get("action", [None])[0]

if action == "save":
    player = params.get("player", [""])[0]
    market = params.get("market", [""])[0]
    line = params.get("line", [""])[0]
    price = params.get("price", [""])[0]
    bookmaker = params.get("bookmaker", [""])[0]
    uid = params.get("id", [""])[0]

    if uid and uid not in st.session_state.processed_actions:
        st.session_state.saved_bets.append(
            {
                "player": player,
                "market": market,
                "line": line,
                "price": price,
                "bookmaker": bookmaker,
            }
        )
        st.session_state.processed_actions.add(uid)
        st.success(f"Saved bet: {player} {market} {line} @ {price} ({bookmaker})")

    cleaned = params.copy()
    for k in ["action", "player", "market", "line", "price", "bookmaker", "id"]:
        cleaned.pop(k, None)

    st.query_params = cleaned

# Handle navigation to Trend tab via query params
go = params.get("go", [None])[0]
if go == "trend":
    player = params.get("player", [""])[0]
    market = params.get("market", [""])[0]
    line_s = params.get("line", [""])[0]

    st.session_state.trend_player = player or None
    st.session_state.trend_market = market or None
    try:
        st.session_state.trend_line = float(line_s)
    except Exception:
        st.session_state.trend_line = None

    st.session_state.active_tab = "📈 Trend Analysis"

    cleaned = params.copy()
    for k in ["go", "player", "market", "line"]:
        cleaned.pop(k, None)
    st.experimental_set_query_params(**cleaned)
    params = cleaned

# ------------------------------------------------------
# UTILITY FUNCTIONS
# ------------------------------------------------------
def format_moneyline(v):
    try:
        v = float(v)
        v = int(round(v))
        return f"+{v}" if v > 0 else str(v)
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
        col = f"{stat}_last{n}"
        return row.get(col, np.nan)

    df["L5 Avg"] = df.apply(lambda r: pull(r, 5), axis=1)
    df["L10 Avg"] = df.apply(lambda r: pull(r, 10), axis=1)
    df["L20 Avg"] = df.apply(lambda r: pull(r, 20), axis=1)
    return df

def add_defense(df):
    df = df.copy()
    stat = df["market"].apply(detect_stat)

    pos = {
        "pts": "opp_pos_pts_rank",
        "reb": "opp_pos_reb_rank",
        "ast": "opp_pos_ast_rank",
        "pra": "opp_pos_pra_rank",
    }
    overall = {
        "pts": "opp_overall_pts_rank",
        "reb": "opp_overall_reb_rank",
        "ast": "opp_overall_ast_rank",
        "pra": "opp_overall_pra_rank",
    }

    df["Pos Def Rank"] = [
        df.loc[i, pos.get(stat[i])] if pos.get(stat[i]) in df.columns else ""
        for i in df.index
    ]
    df["Overall Def Rank"] = [
        df.loc[i, overall.get(stat[i])] if overall.get(stat[i]) in df.columns else ""
        for i in df.index
    ]
    df["Matchup Difficulty"] = df.get("matchup_difficulty_score", np.nan)
    return df

def format_display(df):
    df = df.copy()

    # Round matchup difficulty but keep numeric semantics
    df["Matchup Difficulty"] = df["Matchup Difficulty"].apply(
        lambda x: int(round(x)) if pd.notna(x) else ""
    )

    for col in ["hit_rate_last5", "hit_rate_last10", "hit_rate_last20"]:
        def fmt(x):
            if pd.isna(x):
                return ""
            if 0 <= x <= 1:
                return f"{int(round(x * 100))}%"
            return f"{int(round(x))}%"

        df[col] = df[col].apply(fmt)

    for col in ["L5 Avg", "L10 Avg", "L20 Avg"]:
        df[col] = df[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")

    return df

# ------------------------------------------------------
# LOAD DATA
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

# NEW: toggle to only show saved props
show_only_saved = st.sidebar.checkbox("Show Only Saved Props", value=False)

# ------------------------------------------------------
# REFRESH BUTTON
# ------------------------------------------------------
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

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

    # If toggle on, restrict to saved props (match by player, market, line, bookmaker)
    if show_only_saved and st.session_state.saved_bets:
        saved_df = pd.DataFrame(st.session_state.saved_bets)
        d = d.merge(
            saved_df[["player", "market", "line", "bookmaker"]],
            on=["player", "market", "line", "bookmaker"],
            how="inner",
        )

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
# TAB 1 — PROPS OVERVIEW (HTML + DataTables + Save + Clickable Player)
# ------------------------------------------------------
import streamlit.components.v1 as components

if current_tab == "🧮 Props Overview":
    st.subheader("Props Overview")

    d = filter_props(props_df)

    if d.empty:
        st.info("No props match your filters.")
    else:
        # Enhance dataset
        d = get_dynamic_averages(d)
        d = add_defense(d)
        d["Price"] = d["price"].apply(format_moneyline)
        d = format_display(d)
        d["Opponent Logo URL"] = d["opponent_team"].apply(lambda t: TEAM_LOGOS.get(t, ""))

        # Default sort: hit_rate_last10 descending (numeric)
        d = d.sort_values(
            by="hit_rate_last10",
            ascending=False,
            key=lambda s: pd.to_numeric(s.str.rstrip('%'), errors="coerce")
        )

        # ---------- Build table HTML ----------
        header_html = """
        <thead>
        <tr>
            <th>Save Bet</th>
            <th>Player</th>
            <th>Market</th>
            <th>Line</th>
            <th>Price</th>
            <th>Book</th>
            <th>Pos Def Rank</th>
            <th>Overall Def Rank</th>
            <th>Matchup</th>
            <th>Hit L5</th>
            <th>Hit L10</th>
            <th>Hit L20</th>
            <th>L5 Avg</th>
            <th>L10 Avg</th>
            <th>L20 Avg</th>
            <th>Opponent</th>
        </tr>
        </thead>
        """

        body_html = "<tbody>"

        for idx, row in d.iterrows():

            # Save button
            save_params = {
                "action": "save",
                "player": row["player"],
                "market": row["market"],
                "line": row["line"],
                "price": row["Price"],
                "bookmaker": row["bookmaker"],
                "id": f"{row['player']}_{row['market']}_{row['line']}_{idx}",
            }
            save_href = "?" + urlencode(save_params)
            save_button = (
                f"<a href='{save_href}' "
                "style='background:#28a745;color:white;padding:4px 8px;"
                "border-radius:4px;text-decoration:none;font-size:12px;'>Save</a>"
            )

            # Trend link
            trend_params = {
                "go": "trend",
                "player": row["player"],
                "market": row["market"],
                "line": row["line"],
            }
            trend_href = "?" + urlencode(trend_params)
            player_link = (
                f"<a href='{trend_href}' "
                "style='color:#4da6ff;text-decoration:underline;'>"
                f"{html_lib.escape(str(row['player']))}</a>"
            )

            # Opponent logo
            if row["Opponent Logo URL"]:
                logo_html = f"<img src='{row['Opponent Logo URL']}' width='32' style='display:block;margin:auto;'/>"
            else:
                logo_html = ""

            # Matchup difficulty color coding
            match_val = row["Matchup Difficulty"]
            try:
                mv = float(match_val)
                if mv <= 20:
                    bg = "background-color:#5cb85c;color:white;"
                elif mv <= 40:
                    bg = "background-color:#ffd500;color:black;"
                elif mv <= 60:
                    bg = "background-color:#aaaaaa;color:black;"
                elif mv <= 80:
                    bg = "background-color:#f0ad4e;color:black;"
                else:
                    bg = "background-color:#d9534f;color:white;"
            except:
                mv, bg = "", ""

            body_html += f"""
            <tr>
                <td>{save_button}</td>
                <td>{player_link}</td>
                <td>{html_lib.escape(str(row['market']))}</td>
                <td>{html_lib.escape(str(row['line']))}</td>
                <td>{html_lib.escape(str(row['Price']))}</td>
                <td>{html_lib.escape(str(row['bookmaker']))}</td>
                <td>{html_lib.escape(str(row['Pos Def Rank']))}</td>
                <td>{html_lib.escape(str(row['Overall Def Rank']))}</td>
                <td style="{bg}">{mv}</td>
                <td>{html_lib.escape(str(row['hit_rate_last5']))}</td>
                <td>{html_lib.escape(str(row['hit_rate_last10']))}</td>
                <td>{html_lib.escape(str(row['hit_rate_last20']))}</td>
                <td>{html_lib.escape(str(row['L5 Avg']))}</td>
                <td>{html_lib.escape(str(row['L10 Avg']))}</td>
                <td>{html_lib.escape(str(row['L20 Avg']))}</td>
                <td>{logo_html}</td>
            </tr>
            """

        body_html += "</tbody>"

        # ---------- Final HTML with DataTables ----------
        full_table = f"""
        <html>
        <head>
            <link rel="stylesheet"
                href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
            <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
            <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>

            <style>
                table.dataTable tbody td {{
                    text-align: center;
                    vertical-align: middle;
                }}
                table.dataTable thead th {{
                    text-align: center;
                }}
            </style>

            <script>
            function initDataTable() {{
                if (!$.fn.DataTable.isDataTable('#props-table')) {{
                    $('#props-table').DataTable({{
                        pageLength: 50,
                        autoWidth: false
                    }});
                }}
            }}

            document.addEventListener("DOMContentLoaded", function() {{
                setTimeout(initDataTable, 300);
            }});
            </script>
        </head>

        <body>
            <table id="props-table" class="display" style="width:100%;border-collapse:collapse;">
                {header_html}
                {body_html}
            </table>
        </body>
        </html>
        """

        # USE STREAMLIT COMPONENTS TO RENDER FULL HTML
        components.html(full_table, height=900, scrolling=True)

# ------------------------------------------------------
# TAB 2 — TREND ANALYSIS
# ------------------------------------------------------
elif current_tab == "📈 Trend Analysis":
    st.subheader("Trend Analysis")

    all_players = ["(select)"] + sorted(props_df["player"].unique())
    if st.session_state.trend_player in all_players:
        default_p_index = all_players.index(st.session_state.trend_player)
    else:
        default_p_index = 0

    p = st.selectbox("Player", all_players, index=default_p_index)

    if p != "(select)":
        markets = sorted(props_df[props_df["player"] == p]["market"].unique())
        if st.session_state.trend_market in markets:
            default_m_index = markets.index(st.session_state.trend_market)
        else:
            default_m_index = 0

        m = st.selectbox("Market", markets, index=default_m_index)

        lines = sorted(
            props_df[(props_df["player"] == p) & (props_df["market"] == m)][
                "line"
            ].unique()
        )

        if (
            st.session_state.trend_line is not None
            and st.session_state.trend_line in lines
        ):
            default_line_index = list(lines).index(st.session_state.trend_line)
        else:
            default_line_index = 0

        line_pick = st.selectbox("Select Line", lines, index=default_line_index)

        stat = detect_stat(m)

        df_hist = (
            historical_df[
                (historical_df["player"] == p) & (historical_df[stat].notna())
            ]
            .sort_values("game_date")
            .tail(20)
        )

        df_hist["date"] = df_hist["game_date"].dt.strftime("%b %d")
        df_hist["color"] = np.where(df_hist[stat] > line_pick, "green", "red")

        hover = [
            f"<b>{d}</b><br>{stat.upper()}: {v}<br>Opponent: {opp}"
            for d, opp, v in zip(
                df_hist["date"], df_hist["opponent_team"], df_hist[stat]
            )
        ]

        fig = go.Figure()

        fig.add_bar(
            x=df_hist["date"],
            y=df_hist[stat],
            marker_color=df_hist["color"],
            hovertext=hover,
            hoverinfo="text",
        )

        fig.update_xaxes(tickvals=df_hist["date"], ticktext=df_hist["date"])

        for date_label, opp_team in zip(df_hist["date"], df_hist["opponent_team"]):
            logo_url = TEAM_LOGOS.get(opp_team, "")
            if logo_url:
                fig.add_layout_image(
                    dict(
                        source=logo_url,
                        xref="x",
                        yref="paper",
                        x=date_label,
                        y=-0.15,
                        sizex=0.2,
                        sizey=0.2,
                        xanchor="center",
                        yanchor="top",
                    )
                )

        fig.add_hline(
            y=line_pick,
            line_dash="dash",
            line_color="white",
            annotation_text=f"Line: {line_pick}",
            annotation_position="top left",
        )

        fig.update_layout(
            height=450,
            plot_bgcolor="#222",
            paper_bgcolor="#222",
            font=dict(color="white"),
            margin=dict(b=80, t=40, l=40, r=20),
        )

        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------
# TAB 3 — SAVED BETS
# ------------------------------------------------------
elif current_tab == "📋 Saved Bets":
    st.subheader("Saved Bets")

    if not st.session_state.saved_bets:
        st.info("No saved bets yet.")
    else:
        df_save = pd.DataFrame(st.session_state.saved_bets)
        st.dataframe(df_save, use_container_width=True)

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
        d = format_display(d)
        d["Price"] = d["price"].apply(format_moneyline)

        # --- NEW EV columns ---
        ev_cols = ["ev_last5", "ev_last10", "ev_last20"]

        missing = [c for c in ev_cols if c not in d.columns]
        if missing:
            st.error(f"❌ Missing EV columns in database: {', '.join(missing)}")
            st.stop()

        # Convert EV columns to numeric
        for col in ev_cols:
            d[col] = pd.to_numeric(d[col], errors="coerce")

        # Sort by EV (highest EV_last10 default)
        d = d.sort_values("ev_last10", ascending=False)

        # Format hit rates
        d["Hit Rate 10"] = d["hit_rate_last10"]

        # Columns to display
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

        st.dataframe(d[cols], use_container_width=True, hide_index=True)
