# ======================================================
# GOAT NBA UNIFIED INGESTION SERVICE
# ======================================================

import os
import time
import json
import requests
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# ======================================================
# CONFIG
# ======================================================
PROJECT_ID = os.getenv("PROJECT_ID", "graphite-flare-477419-h7")
DATASET = os.getenv("GOAT_DATASET", "nba_goat_data")

# ======================================================
# TABLES
# ======================================================
TABLE_PLAYER_INJURIES = "player_injuries"
TABLE_PLAYER_INJURIES_STAGING = "player_injuries_staging"
TABLE_STATE = "ingest_state"
TABLE_ACTIVE_PLAYERS = "active_players"
TABLE_GAME_STATS_FULL = "player_game_stats_full"
TABLE_GAME_STATS_PERIOD = "player_game_stats_period"
TABLE_LINEUPS = "game_lineups"
TABLE_PLAYER_PROPS = "player_prop_odds"
TABLE_PLAYER_PROPS_STAGING = "player_prop_odds_staging"
TABLE_GAME_STATS_ADVANCED = "player_game_stats_advanced"
TABLE_GAME_STATS_ADVANCED_STAGING = "player_game_stats_advanced_staging"
TABLE_GAME_PLAYS_FIRST3 = "game_plays_first3min"


BALDONTLIE_STATS_BASE = "https://api.balldontlie.io/v1"
BALDONTLIE_NBA_BASE = "https://api.balldontlie.io/v1"
BALDONTLIE_ODDS_BASE = "https://api.balldontlie.io/v2"

API_KEY = os.getenv("BALDONTLIE_KEY", "")
if not API_KEY:
    print("⚠️ BALDONTLIE_KEY missing")

HEADERS = {"Authorization": API_KEY}

RATE_PROFILE = os.getenv("BALLDONTLIE_TIER", "GOAT").upper()
RATE_LIMITS = {
    "ALL_STAR": {"batch": 5, "delay": 1.2, "retry": 10},
    "GOAT": {"batch": 20, "delay": 0.3, "retry": 3},
}
RATE = RATE_LIMITS.get(RATE_PROFILE, RATE_LIMITS["ALL_STAR"])

THROTTLES = {
    "active_players": 3600,
    "stats_full": 600,
    "stats_period": 900,
    "lineups": 120,
    "props": 120,
}
THROTTLES["box_scores"] = 600

TABLE_GAMES = "games"

THROTTLES["games"] = 120

# ======================================================
# APP
# ======================================================
app = Flask(__name__)
bq = bigquery.Client(project=PROJECT_ID)

# ======================================================
# HELPERS
# ======================================================
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def sleep_s(sec: float):
    time.sleep(max(sec, 0))

def table(name: str) -> str:
    return f"{PROJECT_ID}.{DATASET}.{name}"

from requests.exceptions import ReadTimeout, ConnectionError as ReqConnectionError

def http_get(base: str, path: str, params=None, *, max_retries=3):
    url = f"{base}{path}"

    headers = {}
    if "api.balldontlie.io" in base:
        headers["Authorization"] = API_KEY

    for attempt in range(max_retries):
        try:
            r = requests.get(
                url,
                headers=headers,
                params=params or {},
                timeout=25,
            )
        except (ReadTimeout, ReqConnectionError) as e:
            print(f"⚠️ Network error ({attempt+1}/{max_retries}) for {url}: {e}")
            sleep_s(1.5)
            continue

        # Rate limit
        if r.status_code == 429:
            sleep_s(RATE["retry"])
            continue

        # Hard failure
        if not r.ok:
            raise RuntimeError(
                f"HTTP {r.status_code} from {r.url}\n{r.text[:500]}"
            )

        # Content type guard
        content_type = r.headers.get("Content-Type", "")
        if "application/json" not in content_type.lower():
            raise RuntimeError(
                f"NON-JSON RESPONSE from {r.url}\n"
                f"Content-Type: {content_type}\n"
                f"Body:\n{r.text[:500]}"
            )

        return r.json()

    # If we exhaust retries
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")

from datetime import timedelta
import pytz

def yesterday_ny():
    ny = pytz.timezone("America/New_York")
    return (datetime.now(ny).date() - timedelta(days=1)).isoformat()

def parse_start_time_est(g: dict) -> Optional[str]:
    """
    Safely extract game start time in EST from a Ball Don't Lie game payload.
    Prefers true tip time fields and falls back only if needed.
    """
    raw = (
        g.get("start_time")      # preferred
        or g.get("scheduled")    # fallback
        or g.get("date")         # last resort
    )

    if not raw:
        return None

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone(ZoneInfo("America/New_York")).isoformat()
    except Exception:
        return None


def merge_stats_advanced():
    bq.query(
        f"""
        MERGE `{table(TABLE_GAME_STATS_ADVANCED)}` t
        USING `{table(TABLE_GAME_STATS_ADVANCED_STAGING)}` s
        ON
          t.game_id = s.game_id
          AND t.player_id = s.player_id
        WHEN NOT MATCHED THEN
          INSERT ROW
        """
    ).result()

def merge_team_box_scores():
    bq.query(
        f"""
        MERGE `{table(TABLE_TEAM_BOX)}` t
        USING `{table(TABLE_TEAM_BOX_STAGING)}` s
        ON
          t.game_id = s.game_id
          AND t.team_id = s.team_id
        WHEN NOT MATCHED THEN
          INSERT ROW
        """
    ).result()


def paginate(base: str, path: str, params: Dict[str, Any]):
    out, cursor = [], None
    retries = 0

    while True:
        p = dict(params)
        if cursor:
            p["cursor"] = cursor

        try:
            data = http_get(base, path, p)
        except RuntimeError as e:
            # 🔥 Ball Don't Lie pagination occasionally dies
            print(f"⚠️ Pagination error at cursor={cursor}: {e}")

            # retry once
            if retries < 1:
                retries += 1
                sleep_s(1.5)
                continue

            # otherwise stop safely
            break

        retries = 0  # reset on success

        out.extend(data.get("data", []))
        cursor = (data.get("meta") or {}).get("next_cursor")

        if not cursor:
            break

        sleep_s(RATE["delay"])

    return out

from datetime import timedelta

def daterange(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

def bq_overwrite(name: str, rows: list):
    if rows:
        bq.load_table_from_json(
            rows,
            table(name),
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                source_format="NEWLINE_DELIMITED_JSON",
            ),
        ).result()

def bq_replace_by_date(table_name: str, game_date: str, rows: list):
    """
    Idempotent date-scoped overwrite.
    Deletes only the target date, then inserts fresh rows.
    """
    if not rows:
        return

    # 1️⃣ Delete only that date
    bq.query(
        f"""
        DELETE FROM `{table(table_name)}`
        WHERE game_date = @game_date
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "STRING", game_date)
            ]
        ),
    ).result()

    # 2️⃣ Insert fresh rows
    bq_append(table_name, rows)


def swap_tables(final_table: str, staging_table: str):
    bq.query(
        f"""
        CREATE OR REPLACE TABLE `{table(final_table)}` AS
        SELECT * FROM `{table(staging_table)}`
        """
    ).result()

    bq.query(
        f"TRUNCATE TABLE `{table(staging_table)}`"
    ).result()

def is_obvious_fanduel_q1_milestone(
    *,
    vendor: str,
    prop_type_norm: str,
    raw_prop_type: str,
    market_window: str,
    line_value,
    odds,
) -> bool:
    """
    Correct FanDuel Q1 milestone detection.

    FanDuel DOES offer FULL-game 10 & 15 milestones,
    so we must not rely on line value alone.
    """

    # FanDuel only
    if vendor != "fanduel":
        return False

    # Points only
    if prop_type_norm != "points":
        return False

    # Already explicitly FULL → never override
    if market_window == "FULL":
        return False

    # Already explicitly time-scoped → trust it
    if raw_prop_type.endswith(("_1q", "_first3min")):
        return True

    # Milestone ladders have single odds (not O/U)
    if odds is None:
        return False

    try:
        int(float(line_value))
    except (TypeError, ValueError):
        return False

    return True

def apply_q1_prop_type(prop_type: str) -> str:
    """
    Convert base prop_type to Q1-specific prop_type.
    """
    if prop_type == "points":
        return "points_1q"
    if prop_type == "rebounds":
        return "rebounds_1q"
    if prop_type == "assists":
        return "assists_1q"
    return prop_type


# ======================================================
# STATE / THROTTLE
# ======================================================
def ensure_state():
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table(TABLE_STATE)}`
    (
      job_name STRING NOT NULL,
      last_run_ts TIMESTAMP,
      meta STRING
    )
    """
    bq.query(sql).result()

def throttle(job_name: str):
    ensure_state()

    q = f"""
    SELECT last_run_ts
    FROM `{table(TABLE_STATE)}`
    WHERE job_name = @job_name
    LIMIT 1
    """

    rows = list(
        bq.query(
            q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_name", "STRING", job_name)
                ]
            ),
        ).result()
    )

    if not rows or rows[0]["last_run_ts"] is None:
        return True

    age = (datetime.now(timezone.utc) - rows[0]["last_run_ts"].replace(tzinfo=timezone.utc)).total_seconds()
    return age >= THROTTLES[job_name]

def mark_run(job_name: str, meta: dict):
    ensure_state()

    bq.query(
        f"""
        MERGE `{table(TABLE_STATE)}` t
        USING (
          SELECT
            @job_name AS job_name,
            CURRENT_TIMESTAMP() AS last_run_ts,
            @meta AS meta
        ) s
        ON t.job_name = s.job_name
        WHEN MATCHED THEN
          UPDATE SET last_run_ts = s.last_run_ts, meta = s.meta
        WHEN NOT MATCHED THEN
          INSERT (job_name, last_run_ts, meta)
          VALUES (s.job_name, s.last_run_ts, s.meta)
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_name", "STRING", job_name),
                bigquery.ScalarQueryParameter("meta", "STRING", json.dumps(meta)),
            ]
        ),
    ).result()

def minutes_to_seconds(min_str: Optional[str]) -> Optional[int]:
    try:
        m, s = min_str.split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return None

def parse_clock_to_seconds(clock) -> Optional[int]:
    """
    Parse Ball Don't Lie play clock formats.

    Supports:
      - 'MM:SS'   (e.g. '11:43')
      - 'SS.s'    (e.g. '54.2')
      - int / float seconds
    """
    if clock is None:
        return None

    # MM:SS format
    if isinstance(clock, str) and ":" in clock:
        try:
            m, s = clock.split(":")
            return int(m) * 60 + int(float(s))
        except Exception:
            return None

    # Seconds-only format ('54.2', 54.2, etc)
    try:
        return int(float(clock))
    except Exception:
        return None

def clock_to_seconds(clock: str) -> int | None:
    """
    Converts 'MM:SS' to seconds remaining.
    """
    if not clock:
        return None
    try:
        m, s = clock.split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return None

def bq_append(name: str, rows: list):
    if rows:
        bq.load_table_from_json(
            rows,
            table(name),
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                source_format="NEWLINE_DELIMITED_JSON",
            ),
        ).result()

BALDONTLIE_GAMES_BASE = "https://api.balldontlie.io/v1"

def truncate(table_name: str):
    bq.query(f"TRUNCATE TABLE `{table(table_name)}`").result()


def fetch_games_for_date(game_date: str):
    return http_get(
        BALDONTLIE_GAMES_BASE,
        "/games",
        params={"dates[]": game_date},
    ).get("data", [])

def ensure_backfill_log():
    bq.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{table("backfill_log")}` (
            run_id STRING,
            log_ts TIMESTAMP,
            level STRING,
            scope STRING,
            message STRING,
            meta STRING
        )
        """
    ).result()

import uuid

def log_event(
    run_id: str,
    level: str,
    scope: str,
    message: str,
    meta: Optional[dict] = None,
):
    ensure_backfill_log()

    row = {
        "run_id": run_id,
        "log_ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "scope": scope,
        "message": message,
        "meta": json.dumps(meta or {}),
    }

    # stdout (real-time visibility)
    print(f"[{level}] [{scope}] {message}")

    # BigQuery
    bq_append("backfill_log", [row])

import re
from typing import Tuple, Optional

WINDOW_PATTERNS = [
    (r"(?:^|_)(1q|q1|first_quarter|1st_quarter)(?:_|$)", "Q1"),
    (r"(?:^|_)(2q|q2|second_quarter|2nd_quarter)(?:_|$)", "Q2"),
    (r"(?:^|_)(3q|q3|third_quarter|3rd_quarter)(?:_|$)", "Q3"),
    (r"(?:^|_)(4q|q4|fourth_quarter|4th_quarter)(?:_|$)", "Q4"),
    (r"(?:^|_)(1h|h1|first_half|1st_half)(?:_|$)", "H1"),
    (r"(?:^|_)(2h|h2|second_half|2nd_half)(?:_|$)", "H2"),
    (r"(?:^|_)(full|game|match)(?:_|$)", "FULL"),
]

SUFFIX_STRIP_RE = re.compile(r"(_(1q|2q|3q|4q|1h|2h|q1|q2|q3|q4|h1|h2))$", re.IGNORECASE)

def infer_market_window(prop_type: str, market_type: str) -> str:
    """
    Derive market window from prop_type / market_type.
    """
    s = (prop_type or "").lower()

    if s.endswith("_1q") or "_1q_" in s or "_q1" in s:
        return "Q1"

    if s.endswith("_first3min") or "first3min" in s:
        return "Q1"   # or "Q1_OPENING" if you want a separate bucket

    if s.endswith("_1h") or "_first_half" in s:
        return "H1"

    return "FULL"

def base_prop_type(prop_type: str) -> str:
    """
    Normalize prop_type by stripping period / time qualifiers.
    """
    if not prop_type:
        return prop_type

    for suffix in (
        "_1q",
        "_q1",
        "_1h",
        "_first3min",
    ):
        if prop_type.endswith(suffix):
            return prop_type.replace(suffix, "")

    return prop_type

import hashlib

def injury_hash(
    player_id: int,
    status: str,
    description: str,
    return_date: str,
) -> str:
    raw = f"{player_id}|{status}|{description}|{return_date}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

# ======================================================
# ACTIVE PLAYERS
# ======================================================
def ingest_active_players(season: int):
    if not throttle("active_players"):
        return {"status": "throttled"}
    players = paginate(BALDONTLIE_NBA_BASE, "/players/active", {"season": season})
    rows = [{
        "season": season,
        "player_id": p["id"],
        "name": f'{p["first_name"]} {p["last_name"]}',
        "team_id": p.get("team_id"),
        "position": p.get("position"),
        "updated_at": now_iso(),
    } for p in players]
    bq_append(TABLE_ACTIVE_PLAYERS, rows)
    mark_run("active_players", {"rows": len(rows)})
    return {"rows": len(rows)}

# ======================================================
# GAME STATS
# ======================================================
def ingest_stats(
    start: str,
    end: str,
    period: Optional[int],
    *,
    bypass_throttle: bool = False,
):
    job = "stats_period" if period else "stats_full"

    # 🔒 Throttle unless explicitly bypassed (quarters wrapper)
    if not bypass_throttle and not throttle(job):
        return {"status": "throttled"}

    games = paginate(
        BALDONTLIE_NBA_BASE,
        "/games",
        {"start_date": start, "end_date": end},
    )

    rows = []

    for g in games:
        gid = g["id"]

        stats = paginate(
            BALDONTLIE_STATS_BASE,
            "/stats",
            {
                "game_ids[]": gid,
                **({"period": period} if period else {}),
            },
        )

        for s in stats:
            minutes = s.get("min")

            # --------------------------------------------------
            # 🚫 Skip players who did not play
            # --------------------------------------------------
            if not minutes or minutes in ("0:00", "00:00"):
                continue

            seconds_played = minutes_to_seconds(minutes)

            row = {
                # --------------------------------------------------
                # CORE
                # --------------------------------------------------
                "game_id": gid,
                "game_date": g["date"][:10],
                "season": g["season"],

                "player_id": s["player"]["id"],
                "player": f'{s["player"]["first_name"]} {s["player"]["last_name"]}',

                "team_id": s["team"]["id"],
                "team": s["team"]["abbreviation"],

                # --------------------------------------------------
                # PLAYING TIME
                # --------------------------------------------------
                "minutes": minutes,
                "seconds_played": seconds_played,

                # --------------------------------------------------
                # BASIC STATS
                # --------------------------------------------------
                "pts": s.get("pts"),
                "reb": s.get("reb"),
                "ast": s.get("ast"),
                "stl": s.get("stl"),
                "blk": s.get("blk"),

                # --------------------------------------------------
                # POSSESSION / FOULS
                # --------------------------------------------------
                "turnover": s.get("turnover"),
                "pf": s.get("pf"),

                # --------------------------------------------------
                # SHOOTING
                # --------------------------------------------------
                "fgm": s.get("fgm"),
                "fga": s.get("fga"),
                "fg3m": s.get("fg3m"),
                "fg3a": s.get("fg3a"),
                "ftm": s.get("ftm"),
                "fta": s.get("fta"),

                # --------------------------------------------------
                # MISC
                # --------------------------------------------------
                "plus_minus": s.get("plus_minus"),

                # --------------------------------------------------
                # METADATA
                # --------------------------------------------------
                "data_quality": "official",
            }

            # --------------------------------------------------
            # PERIOD NORMALIZATION
            # --------------------------------------------------
            if period:
                row["period"] = f"Q{period}"
                row["period_num"] = period
                row["ingested_at"] = now_iso()

            rows.append(row)

        sleep_s(RATE["delay"])



    bq_append(
        TABLE_GAME_STATS_PERIOD if period else TABLE_GAME_STATS_FULL,
        rows,
    )

    mark_run(job, {"games": len(games), "rows": len(rows)})

    return {"rows": len(rows)}

def ingest_stats_advanced(start: str, end: str, *, bypass_throttle=False):
    job = "stats_advanced"

    if not bypass_throttle and not throttle(job):
        return {"status": "throttled"}

    games = paginate(
        BALDONTLIE_NBA_BASE,
        "/games",
        {"start_date": start, "end_date": end},
    )

    rows = []

    for g in games:
        # Advanced stats only exist for completed games
        if g.get("status") != "Final":
            continue

        stats = paginate(
            BALDONTLIE_STATS_BASE,
            "/stats/advanced",
            {"game_ids[]": g["id"]},
        )

        for s in stats:
            rows.append({
                "game_id": g["id"],
                "game_date": g["date"][:10],
                "season": g["season"],

                "player_id": s["player"]["id"],
                "team_id": s["team"]["id"],

                "pie": s.get("pie"),
                "pace": s.get("pace"),
                "assist_percentage": s.get("assist_percentage"),
                "assist_ratio": s.get("assist_ratio"),
                "assist_to_turnover": s.get("assist_to_turnover"),
                "defensive_rating": s.get("defensive_rating"),
                "defensive_rebound_percentage": s.get("defensive_rebound_percentage"),
                "effective_field_goal_percentage": s.get("effective_field_goal_percentage"),
                "net_rating": s.get("net_rating"),
                "offensive_rating": s.get("offensive_rating"),
                "offensive_rebound_percentage": s.get("offensive_rebound_percentage"),
                "rebound_percentage": s.get("rebound_percentage"),
                "true_shooting_percentage": s.get("true_shooting_percentage"),
                "turnover_ratio": s.get("turnover_ratio"),
                "usage_percentage": s.get("usage_percentage"),

                "data_quality": "official",
                "ingested_at": now_iso(),
            })

        sleep_s(RATE["delay"])

    if rows:
        bq_append(TABLE_GAME_STATS_ADVANCED_STAGING, rows)
        merge_stats_advanced()
        truncate(TABLE_GAME_STATS_ADVANCED_STAGING)

    mark_run(job, {
        "games_checked": len(games),
        "rows_attempted": len(rows),
    })

    return {"rows_attempted": len(rows)}



# ======================================================
# LINEUPS (BDL v1 – CORRECT)
# ======================================================
def ingest_lineups(start: str, end: str):
    job = "lineups"

    if not throttle(job):
        return {"status": "throttled"}

    games = paginate(
        BALDONTLIE_NBA_BASE,
        "/games",
        {"start_date": start, "end_date": end},
    )

    rows = []

    for g in games:
        game_id = g["id"]
        game_date = g["date"][:10]

        lineups = paginate(
            BALDONTLIE_NBA_BASE,
            "/lineups",
            {"game_ids[]": game_id},
        )

        for lu in lineups:
            player = lu.get("player") or {}
            team = lu.get("team") or {}

            if not player:
                continue

            rows.append({
                "game_id": game_id,
                "game_date": game_date,

                "team_id": team.get("id"),
                "team_abbr": team.get("abbreviation"),

                "player_id": player.get("id"),
                "player": f'{player.get("first_name")} {player.get("last_name")}',

                "is_starter": lu.get("starter", False),
                "lineup_position": lu.get("position"),

                "ingested_at": now_iso(),
            })

        sleep_s(RATE["delay"])

    if rows:
        # date-scoped idempotency
        bq_replace_by_date(TABLE_LINEUPS, start, rows)

    mark_run(job, {
        "games": len(games),
        "rows": len(rows),
    })

    return {
        "games": len(games),
        "rows": len(rows),
    }

def classify_games(games):
    live = []
    upcoming = []
    final = []

    for g in games:
        status = g.get("status", "").lower()

        if status in ("in progress", "live"):
            live.append(g["id"])
        elif status == "final":
            final.append(g["id"])
        else:
            upcoming.append(g["id"])

    return live, upcoming, final
    
# ======================================================
# PLAYER PROPS (V2 – CORRECT)
# ======================================================
def ingest_player_props(game_date: str, *, bypass_throttle: bool = False):
    """
    Pull LIVE player props for all NBA games on a given date.
    Snapshot-based ingestion. No vendor filtering.
    """

    if not bypass_throttle and not throttle("props"):
        return {"status": "throttled"}


    # --------------------------------------------------
    # 1️⃣ Get scheduled NBA games for the date
    # --------------------------------------------------
    games_resp = http_get(
        "https://api.balldontlie.io/v1",   # ✅ FIXED BASE
        "/games",
        {"dates[]": game_date},
    )

    games = games_resp.get("data", [])
    if not games:
        print(f"⚠️ No NBA games found for {game_date}")
        return {"status": "no_games"}

    rows = []
    games_with_props = 0

    # --------------------------------------------------
    # 2️⃣ Pull props per game_id (REQUIRED)
    # --------------------------------------------------
    for g in games:
        game_id = g["id"]

        try:
            props_resp = http_get(
                "https://api.balldontlie.io/v2/odds",
                "/player_props",
                {
                    "game_id": game_id,
                    "prop_types[]": [
                        "points",
                        "points_1q",
                        "points_first3min",
                        "assists",
                        "assists_1q",
                        "assists_first3min",
                        "rebounds",
                        "rebounds_1q",
                        "rebounds_first3min",
                        "points_rebounds",
                        "points_assists",
                        "points_rebounds_assists",
                        "double_double",
                        "triple_double",
                    ],
                },
            )


        except Exception as e:
            print(f"❌ Failed props pull for game {game_id}: {e}")
            continue

        props = props_resp.get("data", [])
        if not props:
            print(f"ℹ️ No props available for game {game_id}")
            continue

        games_with_props += 1

        # --------------------------------------------------
        # 3️⃣ Normalize props
        # --------------------------------------------------
        for p in props:
            market = p.get("market") or {}

            raw_prop_type = p.get("prop_type") or ""
            mkt_type = market.get("type") or ""

            market_window = infer_market_window(raw_prop_type, mkt_type)

            # Default normalization
            final_prop_type = raw_prop_type
            prop_type_base = base_prop_type(raw_prop_type)

            # 🔴 FanDuel milestone ladder override (Q1)
            if is_obvious_fanduel_q1_milestone(
                vendor=p["vendor"],
                prop_type_norm=prop_type_norm,
                raw_prop_type=raw_prop_type,
                market_window=market_window,
                line_value=p.get("line_value"),
                odds=market.get("odds"),
            ):
                market_window = "Q1"

                # 👇 explicit, provider-scoped rename
                final_prop_type = f"{raw_prop_type}_1q_fanduel"

                # 👇 base stat stays clean
                prop_type_base = raw_prop_type



            rows.append({
                "prop_id": p["id"],
                "game_id": p["game_id"],
                "player_id": p["player_id"],
                "vendor": p["vendor"],

                # ✅ corrected prop_type
                "prop_type": final_prop_type,
                "prop_type_base": prop_type_base,

                "market_window": market_window,
                "line_value": p["line_value"],
                "market_type": mkt_type,

                "odds_over": market.get("over_odds"),
                "odds_under": market.get("under_odds"),
                "milestone_odds": market.get("odds"),

                "updated_at": p["updated_at"],
                "snapshot_ts": now_iso(),
                "ingested_at": now_iso(),
            })



        sleep_s(RATE["delay"])

    # --------------------------------------------------
    # 4️⃣ Write snapshot rows
    # --------------------------------------------------
    if rows:
        bq_overwrite(TABLE_PLAYER_PROPS_STAGING, rows)
        swap_tables(TABLE_PLAYER_PROPS, TABLE_PLAYER_PROPS_STAGING)


    mark_run("props", {
        "date": game_date,
        "games_checked": len(games),
        "games_with_props": games_with_props,
        "rows": len(rows),
    })

    return {
        "date": game_date,
        "games_checked": len(games),
        "games_with_props": games_with_props,
        "rows_inserted": len(rows),
    }

from zoneinfo import ZoneInfo

def ingest_games(
    game_date: Optional[str] = None,
    *,
    bypass_throttle: bool = False,
):
    """
    Snapshot ingest of NBA games (game-level only).
    Fully idempotent per game_date.
    Handles multiple BDL response formats.
    """

    if not bypass_throttle and not throttle("games"):
        return {"status": "throttled"}


    # --------------------------------------------------
    # IDPOTENT DATE SCOPE
    # --------------------------------------------------
    run_date = game_date or yesterday_ny()

    params = {"dates[]": run_date}

    games = paginate(
        BALDONTLIE_GAMES_BASE,
        "/games",
        params,
    )

    rows = []

    for g in games:
        # ---------------- START TIME (EST) ----------------
        start_est = parse_start_time_est(g)


        # ---------------- QUARTER SCORING ----------------
        home_q = [
            g.get("home_q1"),
            g.get("home_q2"),
            g.get("home_q3"),
            g.get("home_q4"),
        ]
        away_q = [
            g.get("visitor_q1"),
            g.get("visitor_q2"),
            g.get("visitor_q3"),
            g.get("visitor_q4"),
        ]

        home_ot = [
            g.get("home_ot1"),
            g.get("home_ot2"),
            g.get("home_ot3"),
            g.get("home_ot4"),
        ]
        away_ot = [
            g.get("visitor_ot1"),
            g.get("visitor_ot2"),
            g.get("visitor_ot3"),
            g.get("visitor_ot4"),
        ]

        home_q = [x for x in home_q if x is not None]
        away_q = [x for x in away_q if x is not None]
        home_ot = [x for x in home_ot if x is not None]
        away_ot = [x for x in away_ot if x is not None]

        # Fallback to legacy arrays
        if not home_q:
            home_q = g.get("home_team_scores") or []
        if not away_q:
            away_q = g.get("visitor_team_scores") or []

        def q(scores, idx):
            return scores[idx] if len(scores) > idx else None

        rows.append({
            "game_id": g["id"],
            "season": g["season"],
            "game_date": run_date,
            "start_time_est": start_est,

            "status": g["status"],
            "is_final": g["status"] == "Final",
            "has_overtime": len(home_ot) > 0,
            "num_overtimes": len(home_ot),

            "home_team_id": g["home_team"]["id"],
            "home_team_abbr": g["home_team"]["abbreviation"],
            "away_team_id": g["visitor_team"]["id"],
            "away_team_abbr": g["visitor_team"]["abbreviation"],

            "home_score_q1": q(home_q, 0),
            "home_score_q2": q(home_q, 1),
            "home_score_q3": q(home_q, 2),
            "home_score_q4": q(home_q, 3),
            "home_score_ot": home_ot,
            "home_score_final": g.get("home_team_score"),

            "away_score_q1": q(away_q, 0),
            "away_score_q2": q(away_q, 1),
            "away_score_q3": q(away_q, 2),
            "away_score_q4": q(away_q, 3),
            "away_score_ot": away_ot,
            "away_score_final": g.get("visitor_team_score"),

            "last_updated": now_iso(),
            "ingested_at": now_iso(),
        })

    # ---------------- SAFE DATE-SCOPED IDEMPOTENT WRITE ----------------
    if not rows:
        print(f"ℹ️ No games found for {run_date}, skipping write")
        mark_run("games", {
            "games": 0,
            "date": run_date,
            "status": "no_rows",
        })
        return {
            "games": 0,
            "date": run_date,
            "status": "no_rows",
        }

    bq_replace_by_date(TABLE_GAMES, run_date, rows)


    mark_run("games", {
        "games": len(rows),
        "date": run_date,
    })

    return {
        "games": len(rows),
        "date": run_date,
    }
 
def ingest_game_plays_first3min(
    game_date: str,
    *,
    bypass_throttle: bool = False,
):
    job = "plays_first3min"

    if not bypass_throttle and not throttle(job):
        print(f"[DEBUG] {job} throttled")
        return {"status": "throttled"}

    games = fetch_games_for_date(game_date)
    print(f"[DEBUG] {game_date} → games returned: {len(games)}")

    rows = []
    now_ts = datetime.utcnow().isoformat()

    for g in games:
        game_id = g.get("id")
        if not game_id:
            continue

        plays = http_get(
            BALDONTLIE_GAMES_BASE,   # MUST BE v1
            "/plays",
            {
                "game_id": game_id,
                "per_page": 200,
            },
        ).get("data", [])

        for p in plays:
            # -----------------------------
            # Guard: period 1 only
            # -----------------------------
            period = p.get("period")
            if period != 1:
                continue

            clock = p.get("clock")
            if not clock or ":" not in clock:
                continue

            try:
                mins, secs = clock.split(":")
                seconds_remaining = int(mins) * 60 + int(secs)
            except Exception:
                continue

            # First 3 minutes only (12:00 → 9:00)
            if seconds_remaining < 540:
                continue

            team = p.get("team") or {}

            row = {
                "game_id": game_id,
                "game_date": game_date,

                "play_id": f"{game_id}_{p.get('order')}",
                "play_order": p.get("order"),

                "play_type": p.get("type"),
                "play_text": p.get("text"),

                "period": period,
                "clock": clock,
                "seconds_remaining": seconds_remaining,

                "scoring_play": p.get("scoring_play", False),
                "shooting_play": p.get("shooting_play", False),
                "score_value": p.get("score_value"),

                "team_id": team.get("id"),
                "team_abbr": team.get("abbreviation"),

                # Derived flags (basic)
                "is_tipoff": p.get("type") == "Jumpball",
                "is_made_shot": p.get("shooting_play") and p.get("scoring_play"),
                "is_missed_shot": p.get("shooting_play") and not p.get("scoring_play"),
                "is_first_basket": False,  # computed after ingest

                "wallclock": p.get("wallclock"),
                "ingested_at": now_ts,
            }

            rows.append(row)

    print(f"[DEBUG] rows surviving filters: {len(rows)}")

    if rows:
        bq_replace_by_date("game_plays_first3min", game_date, rows)

    mark_run(job, {"date": game_date})
    return {
        "status": "ok",
        "games": len(games),
        "rows": len(rows),
    }

BALDONTLIE_BASE = "https://api.balldontlie.io/v1"

def fetch_all_player_injuries() -> list[dict]:
    rows = []
    cursor = None

    headers = {
        "Authorization": os.getenv("BALDONTLIE_KEY"),
    }

    while True:
        params = {"per_page": 100}
        if cursor:
            params["cursor"] = cursor

        r = requests.get(
            f"{BALDONTLIE_BASE}/player_injuries",
            headers=headers,
            params=params,
            timeout=30,
        )
        r.raise_for_status()

        payload = r.json()
        data = payload.get("data", [])
        meta = payload.get("meta", {})

        rows.extend(data)

        cursor = meta.get("next_cursor")
        if not cursor:
            break

    return rows
    
def upsert_player_injuries(injuries: list[dict]):
    if not injuries:
        return 0

    bq = bigquery.Client(project=PROJECT_ID)
    table = f"{PROJECT_ID}.{DATASET}.player_injuries"

    now = datetime.now(timezone.utc)

    rows = []

    for item in injuries:
        p = item.get("player", {})

        status = item.get("status")
        description = item.get("description")
        return_date = item.get("return_date")

        ihash = injury_hash(
            player_id=p.get("id"),
            status=status,
            description=description,
            return_date=return_date,
        )

        rows.append({
            "player_id": p.get("id"),
            "team_id": p.get("team_id"),

            "first_name": p.get("first_name"),
            "last_name": p.get("last_name"),
            "position": p.get("position"),
            "jersey_number": p.get("jersey_number"),

            "injury_status": status,
            "injury_description": description,
            "expected_return": return_date,

            "injury_hash": ihash,
            "source": "balldontlie",
            "updated_at": now.isoformat(),
        })

    staging = f"{table}_staging"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("player_id", "INT64"),
            bigquery.SchemaField("team_id", "INT64"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("position", "STRING"),
            bigquery.SchemaField("jersey_number", "STRING"),
            bigquery.SchemaField("injury_status", "STRING"),
            bigquery.SchemaField("injury_description", "STRING"),
            bigquery.SchemaField("expected_return", "STRING"),
            bigquery.SchemaField("injury_hash", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ],
    )

    bq.load_table_from_json(rows, staging, job_config=job_config).result()

    merge_sql = f"""
    MERGE `{table}` T
    USING `{staging}` S
    ON T.injury_hash = S.injury_hash
    WHEN NOT MATCHED THEN
      INSERT ROW
    """

    bq.query(merge_sql).result()

    return len(rows)

# ======================================================
# BACKFILL
# ======================================================
from datetime import timedelta

def backfill_season(
    start: str,
    end: str,
    *,
    include_full=True,
    include_quarters=True,
    include_advanced=True,
):
    run_id = f"season_backfill_{uuid.uuid4().hex[:8]}"

    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()

    totals = {
        "days": 0,
        "full_rows": 0,
        "quarter_rows": 0,
        "advanced_rows": 0,
    }

    log_event(
        run_id,
        "INFO",
        "INIT",
        f"Starting season backfill {start} → {end}",
        totals,
    )

    for d in daterange(start_d, end_d):
        day = d.isoformat()
        totals["days"] += 1

        log_event(run_id, "INFO", "DAY_START", f"Processing {day}")

        # ----------------------------------
        # FULL GAME STATS
        # ----------------------------------
        if include_full:
            try:
                r = ingest_stats(
                    start=day,
                    end=day,
                    period=None,
                    bypass_throttle=True,
                )
                rows = r.get("rows", 0)
                totals["full_rows"] += rows

                log_event(
                    run_id,
                    "SUCCESS",
                    "STATS_FULL",
                    f"{day} → {rows} rows",
                )
            except Exception as e:
                log_event(
                    run_id,
                    "ERROR",
                    "STATS_FULL",
                    f"{day} failed",
                    {"error": str(e)},
                )

        # ----------------------------------
        # QUARTERS
        # ----------------------------------
        if include_quarters:
            for q in (1, 2, 3, 4):
                try:
                    r = ingest_stats(
                        start=day,
                        end=day,
                        period=q,
                        bypass_throttle=True,
                    )
                    rows = r.get("rows", 0)
                    totals["quarter_rows"] += rows

                    log_event(
                        run_id,
                        "SUCCESS",
                        f"STATS_Q{q}",
                        f"{day} Q{q} → {rows} rows",
                    )
                except Exception as e:
                    log_event(
                        run_id,
                        "ERROR",
                        f"STATS_Q{q}",
                        f"{day} Q{q} failed",
                        {"error": str(e)},
                    )

        # ----------------------------------
        # ADVANCED
        # ----------------------------------
        if include_advanced:
            try:
                r = ingest_stats_advanced(
                    start=day,
                    end=day,
                    bypass_throttle=True,
                )
                rows = r.get("rows_attempted", 0)
                totals["advanced_rows"] += rows

                log_event(
                    run_id,
                    "SUCCESS",
                    "STATS_ADVANCED",
                    f"{day} → {rows} rows",
                )
            except Exception as e:
                log_event(
                    run_id,
                    "ERROR",
                    "STATS_ADVANCED",
                    f"{day} failed",
                    {"error": str(e)},
                )

        # 🛡️ Hard safety buffer
        sleep_s(1.0)

    log_event(
        run_id,
        "SUCCESS",
        "COMPLETE",
        "Season backfill completed",
        totals,
    )

    return {
        "run_id": run_id,
        **totals,
    }

def backfill_games(
    start: str,
    end: str,
    *,
    sleep_seconds: float = 1.0,
):
    """
    Backfill NBA games table day-by-day.
    Fully idempotent.
    """

    run_id = f"games_backfill_{uuid.uuid4().hex[:8]}"

    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()

    totals = {
        "days": 0,
        "games": 0,
    }

    log_event(
        run_id,
        "INFO",
        "INIT",
        f"Starting games backfill {start} → {end}",
        totals,
    )

    for d in daterange(start_d, end_d):
        day = d.isoformat()
        totals["days"] += 1

        try:
            result = ingest_games(day, bypass_throttle=True)
            games = result.get("games", 0)
            totals["games"] += games

            log_event(
                run_id,
                "SUCCESS",
                "GAMES",
                f"{day} → {games} games",
            )
        except Exception as e:
            log_event(
                run_id,
                "ERROR",
                "GAMES",
                f"{day} failed",
                {"error": str(e)},
            )

        # Safety buffer
        sleep_s(sleep_seconds)

    log_event(
        run_id,
        "SUCCESS",
        "COMPLETE",
        "Games backfill completed",
        totals,
    )

    return {
        "run_id": run_id,
        **totals,
    }


# ======================================================
# ROUTES
# ======================================================
@app.route("/")
def health():
    return "🏀 GOAT NBA ingestion alive"

@app.route("/goat/ingest/player-props")
def route_props():
    date_q = request.args.get("date") or date.today().isoformat()
    bypass = request.args.get("bypass", "false").lower() == "true"
    return jsonify(ingest_player_props(date_q, bypass_throttle=bypass))


@app.route("/goat/ingest/active-players")
def route_active_players():
    season = request.args.get("season", type=int)

    if not season:
        return jsonify({
            "error": "Missing required query param: season"
        }), 400

    result = ingest_active_players(season)
    return jsonify(result)

@app.route("/goat/ingest/lineups")
def route_lineups():
    start = request.args.get("start")
    end = request.args.get("end")

    if not start or not end:
        return jsonify({
            "error": "Missing required query params: start, end (YYYY-MM-DD)"
        }), 400

    return jsonify(ingest_lineups(start, end))

@app.route("/goat/ingest/stats/advanced")
def route_stats_advanced():
    start = request.args.get("start") or yesterday_ny()
    end = request.args.get("end") or start  
    bypass = request.args.get("bypass", "false").lower() == "true"

    return jsonify(
        ingest_stats_advanced(start, end, bypass_throttle=bypass)
    )

def classify_games_by_state(games):
    live, upcoming, final = [], [], []

    for g in games:
        status = (g.get("status") or "").lower()

        if status in ("in progress", "live"):
            live.append(g["id"])
        elif status == "final":
            final.append(g["id"])
        else:
            upcoming.append(g["id"])

    return live, upcoming, final

def test_raw_game_odds(game_ids: list[int], label: str):
    results = {}

    for gid in game_ids:
        try:
            payload = http_get(
                BALDONTLIE_ODDS_BASE,
                "/odds",
                {"game_ids[]": gid},
            )
            results[gid] = payload
        except Exception as e:
            results[gid] = {"error": str(e)}

        sleep_s(0.3)

    # Save locally or log
    with open(f"/tmp/odds_test_{label}.json", "w") as f:
        json.dump(results, f, indent=2)

    return results
    
def run_odds_diagnostic_for_today():
    today = date.today().isoformat()

    games = fetch_games_for_date(today)
    if not games:
        print("⚠️ No games today")
        return

    live_ids, upcoming_ids, final_ids = classify_games_by_state(games)

    print(f"Live: {live_ids}")
    print(f"Upcoming: {upcoming_ids}")
    print(f"Final: {final_ids}")

    if live_ids:
        test_raw_game_odds(live_ids[:2], "live")

    if upcoming_ids:
        test_raw_game_odds(upcoming_ids[:2], "upcoming")

    if final_ids:
        test_raw_game_odds(final_ids[:2], "final")
        

# ======================================================
# GAME STATS ROUTES
# ======================================================

@app.route("/goat/ingest/stats/full")
def route_stats_full():
    start = request.args.get("start") or yesterday_ny()
    end = request.args.get("end") or start

    return jsonify(ingest_stats(start, end, period=None))




@app.route("/goat/ingest/stats/period")
def route_stats_period():
    start = request.args.get("start")
    end = request.args.get("end")
    period = request.args.get("period", type=int)

    if not start or not end or not period:
        return jsonify({
            "error": "Missing required query params: start, end, period"
        }), 400

    if period not in (1, 2, 3, 4):
        return jsonify({
            "error": "period must be 1, 2, 3, or 4"
        }), 400

    return jsonify(ingest_stats(start, end, period=period))

@app.route("/goat/ingest/stats/quarters")
def route_stats_all_quarters():
    start = request.args.get("start") or yesterday_ny()
    end = request.args.get("end") or start


    total_rows = 0

    for q in (1, 2, 3, 4):
        result = ingest_stats(start, end, period=q, bypass_throttle=True)
        total_rows += result.get("rows", 0)

    return {
        "quarters": [1, 2, 3, 4],
        "rows_inserted": total_rows,
    }

@app.route("/goat/ingest/backfill/season")
def route_backfill_season():
    start = request.args.get("start")
    end = request.args.get("end")

    if not start or not end:
        return {"error": "Missing start/end"}, 400

    result = backfill_season(start, end)
    return jsonify(result)

def run_season_backfill_cli():
    start = os.getenv("BACKFILL_START")
    end = os.getenv("BACKFILL_END")

    if not start or not end:
        raise RuntimeError("BACKFILL_START and BACKFILL_END required")

    result = backfill_season(start, end)
    print("✅ BACKFILL COMPLETE")
    print(json.dumps(result, indent=2))

@app.route("/goat/ingest/games")
def route_ingest_games():
    game_date = request.args.get("date")
    bypass = request.args.get("bypass", "false").lower() == "true"

    return jsonify(
        ingest_games(
            game_date,
            bypass_throttle=bypass,
        )
    )


@app.route("/goat/ingest/backfill/games")
def route_backfill_games():
    start = request.args.get("start")
    end = request.args.get("end")

    if not start or not end:
        return jsonify({
            "error": "Missing required query params: start, end (YYYY-MM-DD)"
        }), 400

    return jsonify(backfill_games(start, end))

@app.route("/goat/ingest/plays/first3min")
def route_ingest_plays_first3min():
    game_date = request.args.get("date") or yesterday_ny()
    bypass = request.args.get("bypass", "false").lower() == "true"

    return jsonify(
        ingest_game_plays_first3min(
            game_date,
            bypass_throttle=bypass,
        )
    )

@app.route("/goat/ingest/player-injuries", methods=["GET"])
def ingest_player_injuries():
    job = "player_injuries"

    if not throttle(job):
        return jsonify({"status": "throttled"})

    injuries = fetch_all_player_injuries()
    inserted = upsert_player_injuries(injuries)

    return jsonify({
        "status": "ok",
        "rows_seen": len(injuries),
        "rows_inserted": inserted,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

@app.route("/goat/debug/odds-diagnostic")
def route_odds_diagnostic():
    run_odds_diagnostic_for_today()
    return {"status": "ok", "output": "/tmp/odds_test_*.json"}

if __name__ == "__main__":
    if os.getenv("RUN_BACKFILL") == "true":
        run_season_backfill_cli()
    else:
        app.run(host="0.0.0.0", port=8080, debug=True)
