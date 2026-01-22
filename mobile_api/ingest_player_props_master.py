"""
======================================================
PULSE / GOAT — MASTER PLAYER PROP INGEST (WINDOW-AWARE)
======================================================

Notes:
- BallDontLie endpoints:
  - props: v2 /odds/player_props  (single global pull)
- This ingest intentionally DOES NOT call v1 /games
- Only currently available props are ingested (props appear near game time)
- Frontend filters by game_id
- If the API ever *requires* prop_types[] again, this script auto-falls back to a
  broad list (and you can extend it in FALLBACK_PROP_TYPES).
"""

import os
import re
import time
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.exceptions import ReadTimeout, ConnectionError as ReqConnectionError
from google.cloud import bigquery


# -----------------------------
# CONFIG
# -----------------------------
BALDONTLIE_V1 = "https://api.balldontlie.io/v1"
BALDONTLIE_V2 = "https://api.balldontlie.io/v2"

API_KEY = os.getenv("BALDONTLIE_KEY", "")
if not API_KEY:
    print("⚠️ BALDONTLIE_KEY missing")

PROJECT_ID = (
    os.getenv("GCP_PROJECT")
    or os.getenv("GOOGLE_CLOUD_PROJECT")
    or os.getenv("PROJECT_ID")
    or "graphite-flare-477419-h7"
)

DATASET = os.getenv("PROP_DATASET", "nba_live")
TABLE_FINAL = os.getenv("PROP_TABLE_FINAL", "player_prop_odds_master")
TABLE_STAGING = os.getenv("PROP_TABLE_STAGING", "player_prop_odds_master_staging")

WRITE_MODE = os.getenv("WRITE_MODE", "DRY_RUN").upper().strip()
# WRITE_MODE:
# - DRY_RUN      -> no BQ writes, prints summary + samples
# - STAGING_ONLY -> overwrite staging only
# - SWAP         -> overwrite staging then CREATE OR REPLACE final from staging

VENDORS = [v.strip().lower() for v in os.getenv("VENDORS", "fanduel,draftkings").split(",") if v.strip()]
RATE_DELAY_SEC = float(os.getenv("RATE_DELAY_SEC", "0.25"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "25"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


# Broad fallback list if BallDontLie starts requiring prop_types again
FALLBACK_PROP_TYPES = [
    # core
    "points", "rebounds", "assists", "steals", "blocks", "turnovers",
    "three_pointers_made", "fg3m", "threes",
    # combos
    "points_rebounds", "points_assists", "rebounds_assists",
    "points_rebounds_assists",
    # “specials”
    "double_double", "triple_double",
    # time windows (common BDL naming from your old script)
    "points_1q", "rebounds_1q", "assists_1q",
    "points_first3min", "rebounds_first3min", "assists_first3min",
    "points_1h", "rebounds_1h", "assists_1h",
]


# -----------------------------
# HELPERS (time)
# -----------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def today_ny() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


# -----------------------------
# HELPERS (BQ)
# -----------------------------
def get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
    if project:
        return bigquery.Client(project=project)
    return bigquery.Client()

def fqtn(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET}.{table_name}"

def bq_overwrite_json(client: bigquery.Client, table_name: str, rows: List[dict]) -> None:
    if not rows:
        return
    job = client.load_table_from_json(
        rows,
        fqtn(table_name),
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            source_format="NEWLINE_DELIMITED_JSON",
        ),
    )
    job.result()

def bq_swap_from_staging(client: bigquery.Client, final_table: str, staging_table: str) -> None:
    client.query(
        f"""
        CREATE OR REPLACE TABLE `{fqtn(final_table)}` AS
        SELECT * FROM `{fqtn(staging_table)}`
        """
    ).result()
    client.query(f"TRUNCATE TABLE `{fqtn(staging_table)}`").result()


# -----------------------------
# HELPERS (HTTP)
# -----------------------------
def http_get(base: str, path: str, params: Optional[dict] = None, *, max_retries: int = MAX_RETRIES) -> dict:
    url = f"{base}{path}"
    headers = {}
    if "api.balldontlie.io" in base:
        headers["Authorization"] = f"Bearer {API_KEY}"

    last_err = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params or {}, timeout=HTTP_TIMEOUT)
        except (ReadTimeout, ReqConnectionError) as e:
            last_err = e
            print(f"⚠️ Network error ({attempt+1}/{max_retries}) for {url}: {e}")
            time.sleep(1.25)
            continue

        if r.status_code == 429:
            time.sleep(1.5)
            continue

        if not r.ok:
            # Keep body short
            raise RuntimeError(f"HTTP {r.status_code} from {r.url}\n{r.text[:700]}")

        ctype = (r.headers.get("Content-Type") or "").lower()
        if "application/json" not in ctype:
            raise RuntimeError(f"NON-JSON RESPONSE from {r.url}\nContent-Type: {ctype}\nBody:\n{r.text[:700]}")

        return r.json()

    raise RuntimeError(f"Failed after {max_retries} retries: {url}. Last error: {last_err}")


# -----------------------------
# WINDOW DETECTION / NORMALIZATION
# -----------------------------
WINDOW_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"(?:^|_)(first3min|first_3|min3|3min)(?:_|$)", re.I), "FIRST3MIN"),
    (re.compile(r"(?:^|_)(1q|q1|first_quarter|1st_quarter)(?:_|$)", re.I), "Q1"),
    (re.compile(r"(?:^|_)(2q|q2|second_quarter|2nd_quarter)(?:_|$)", re.I), "Q2"),
    (re.compile(r"(?:^|_)(3q|q3|third_quarter|3rd_quarter)(?:_|$)", re.I), "Q3"),
    (re.compile(r"(?:^|_)(4q|q4|fourth_quarter|4th_quarter)(?:_|$)", re.I), "Q4"),
    (re.compile(r"(?:^|_)(1h|h1|first_half|1st_half)(?:_|$)", re.I), "H1"),
    (re.compile(r"(?:^|_)(2h|h2|second_half|2nd_half)(?:_|$)", re.I), "H2"),
    (re.compile(r"(?:^|_)(full|game|match)(?:_|$)", re.I), "FULL"),
]

SUFFIX_STRIP_RE = re.compile(r"(_(first3min|first_3|min3|3min|1q|2q|3q|4q|q1|q2|q3|q4|1h|2h|h1|h2))$", re.I)

def infer_market_window(
    prop_type: str,
    market_type: str,
    market_name: Optional[str] = None,
    market_desc: Optional[str] = None,
) -> str:
    """
    Robust window inference:
    - Prefer explicit suffixes in prop_type (like points_1q / points_first3min)
    - Else look at market_type / market_name / market_desc
    """
    s = (prop_type or "").lower()
    for pat, label in WINDOW_PATTERNS:
        if pat.search(s):
            return label

    m = " ".join([(market_type or ""), (market_name or ""), (market_desc or "")]).lower()
    # human-language hints
    if "first 3" in m or "first three" in m or "first3" in m or "first 3 min" in m:
        return "FIRST3MIN"
    if "1st quarter" in m or "first quarter" in m or "q1" in m:
        return "Q1"
    if "1st half" in m or "first half" in m or "h1" in m:
        return "H1"
    if "2nd half" in m or "second half" in m or "h2" in m:
        return "H2"

    return "FULL"

def base_prop_type(prop_type: str) -> str:
    if not prop_type:
        return prop_type
    return SUFFIX_STRIP_RE.sub("", prop_type)

def normalize_market_key(raw_prop_type: str, market_type: str) -> str:
    """
    Canonical market key used by your app.
    Priority:
      - use base prop_type (since it’s most consistent across books)
      - fall back to market_type
    """
    pt = (base_prop_type(raw_prop_type) or "").lower()
    mt = (market_type or "").lower()

    # common aliases
    mapping = {
        "points": "pts",
        "assists": "ast",
        "rebounds": "reb",
        "steals": "stl",
        "blocks": "blk",
        "turnovers": "tov",
        "three_pointers_made": "3pm",
        "fg3m": "3pm",
        "threes": "3pm",
        "points_rebounds": "pr",
        "points_assists": "pa",
        "rebounds_assists": "ra",
        "points_rebounds_assists": "pra",
        "double_double": "dd",
        "triple_double": "td",
    }

    if pt in mapping:
        return mapping[pt]

    # if prop_type is weird/missing, use market_type
    if mt in mapping:
        return mapping[mt]

    return pt or mt or "unknown"


# -----------------------------
# FANUEL-SPECIFIC “MARKET CORRECTION”
# -----------------------------
def is_fanduel_milestone_shape(market: dict) -> bool:
    """
    FanDuel (and sometimes others) express milestone ladders as:
      - market.odds present (single price)
      - over_odds/under_odds often null
    """
    if not market:
        return False
    odds_single = market.get("odds")
    has_ou = (market.get("over_odds") is not None) or (market.get("under_odds") is not None)
    return (odds_single is not None) and (not has_ou)

def fanduel_corrected_window(
    raw_prop_type: str,
    market_window: str,
    market: dict,
    market_type: str,
    market_name: Optional[str],
    market_desc: Optional[str],
) -> Tuple[str, str]:
    """
    Returns:
      (corrected_window, corrected_prop_type)

    The goal is NOT to rename everything — only to prevent FanDuel “Q1-ish”
    milestone ladders from being shoved into FULL (or vice versa).

    Rules:
    - If prop_type already explicit (_1q / _first3min / _1h), trust it.
    - If market text strongly implies Q1/H1/FIRST3MIN, trust inference.
    - If it’s a milestone shape and market text implies a window, force it.
    - Otherwise keep as-is.
    """
    rp = (raw_prop_type or "").lower()

    # Explicit time-scoped prop_type — never override
    if any(tag in rp for tag in ["_1q", "_q1", "first3min", "_1h", "first_half", "1st_half"]):
        return market_window, raw_prop_type

    # If inference already says it’s not FULL, trust it
    if market_window != "FULL":
        return market_window, raw_prop_type

    # If milestone shape, look harder at market text for hidden windows
    if is_fanduel_milestone_shape(market):
        m = " ".join([(market_type or ""), (market_name or ""), (market_desc or "")]).lower()

        if any(x in m for x in ["q1", "1st quarter", "first quarter"]):
            return "Q1", f"{raw_prop_type}_q1_fanduel"
        if any(x in m for x in ["first 3", "first three", "first3", "first 3 min"]):
            return "FIRST3MIN", f"{raw_prop_type}_first3min_fanduel"
        if any(x in m for x in ["h1", "1st half", "first half"]):
            return "H1", f"{raw_prop_type}_h1_fanduel"

    return market_window, raw_prop_type


# -----------------------------
# DATA SHAPE (OUTPUT ROW)
# -----------------------------
@dataclass
class NormalizedProp:
    prop_id: Any
    game_id: int
    player_id: int
    vendor: str

    # market + window
    prop_type_raw: str
    prop_type: str
    prop_type_base: str
    market_type: str
    market_key: str
    market_window: str

    # line / odds
    line_value: Any
    odds_over: Any
    odds_under: Any
    milestone_odds: Any

    # timestamps
    updated_at: Any
    snapshot_ts: str
    ingested_at: str

    # optional debug context
    market_name: Optional[str] = None
    market_desc: Optional[str] = None

    def to_row(self) -> dict:
        return {
            "prop_id": self.prop_id,
            "game_id": self.game_id,
            "player_id": self.player_id,
            "vendor": self.vendor,

            "prop_type_raw": self.prop_type_raw,
            "prop_type": self.prop_type,
            "prop_type_base": self.prop_type_base,

            "market_type": self.market_type,
            "market_key": self.market_key,
            "market_window": self.market_window,

            "line_value": self.line_value,
            "odds_over": self.odds_over,
            "odds_under": self.odds_under,
            "milestone_odds": self.milestone_odds,

            "updated_at": self.updated_at,
            "snapshot_ts": self.snapshot_ts,
            "ingested_at": self.ingested_at,

            "market_name": self.market_name,
            "market_desc": self.market_desc,
        }


# -----------------------------
# BALLDONTLIE FETCHERS
# -----------------------------
def fetch_all_player_props() -> List[dict]:
    """
    Pull ALL currently available player props across all games
    for the selected vendors.

    This is the canonical master ingest source.
    """
    params: Dict[str, Any] = {}
    for v in VENDORS:
        params.setdefault("vendors[]", []).append(v)

    # Attempt 1: no prop_types (true ALL)
    try:
        payload = http_get(
            BALDONTLIE_V2,
            "/odds/player_props",
            params,
        )
        return payload.get("data", []) or []

    except RuntimeError as e:
        msg = str(e).lower()

        # Fallback if prop_types suddenly required
        needs_types = (
            "prop_types" in msg
            or ("prop type" in msg and "required" in msg)
        )
        if not needs_types:
            raise

        params2 = dict(params)
        params2["prop_types[]"] = FALLBACK_PROP_TYPES

        payload2 = http_get(
            BALDONTLIE_V2,
            "/odds/player_props",
            params2,
        )
        return payload2.get("data", []) or []


# -----------------------------
# NORMALIZER
# -----------------------------
def normalize_prop(p: dict, *, snapshot_ts: str) -> Optional[NormalizedProp]:
    vendor = (p.get("vendor") or "").lower().strip()
    if vendor not in VENDORS:
        return None

    market = p.get("market") or {}
    raw_prop_type = p.get("prop_type") or ""
    market_type = market.get("type") or ""
    market_name = market.get("name") or market.get("title")  # not always present
    market_desc = market.get("description") or market.get("desc")
    
    # 🔎 TEMP DEBUG — window discovery
    if vendor in ("fanduel", "draftkings"):
        m = p.get("market") or {}
        if m.get("name") or m.get("description"):
            print({
                "vendor": vendor,
                "prop_type": raw_prop_type,
                "market_type": m.get("type"),
                "market_name": m.get("name"),
                "market_desc": m.get("description"),
            })

    # Window inference
    window = infer_market_window(raw_prop_type, market_type, market_name, market_desc)

    # FanDuel correction (only for vendor=fanduel)
    corrected_prop_type = raw_prop_type
    corrected_window = window
    if vendor == "fanduel":
        corrected_window, corrected_prop_type = fanduel_corrected_window(
            raw_prop_type=raw_prop_type,
            market_window=window,
            market=market,
            market_type=market_type,
            market_name=market_name,
            market_desc=market_desc,
        )

    # Base prop type
    pt_base = base_prop_type(corrected_prop_type)

    # Canonical market key your app can rely on
    market_key = normalize_market_key(corrected_prop_type, market_type)

    return NormalizedProp(
        prop_id=p.get("id"),
        game_id=int(p.get("game_id")),
        player_id=int(p.get("player_id")),
        vendor=vendor,

        prop_type_raw=raw_prop_type,
        prop_type=corrected_prop_type,
        prop_type_base=pt_base,
        market_type=str(market_type),
        market_key=market_key,
        market_window=corrected_window,

        line_value=p.get("line_value"),
        odds_over=market.get("over_odds"),
        odds_under=market.get("under_odds"),
        milestone_odds=market.get("odds"),

        updated_at=p.get("updated_at"),
        snapshot_ts=snapshot_ts,
        ingested_at=now_iso(),

        market_name=market_name,
        market_desc=market_desc,
    )


# -----------------------------
# MASTER INGEST
# -----------------------------
def ingest_player_props_master(game_date: str) -> dict:
    """
    Snapshot ingest for a date:
      - pulls games (v1)
      - pulls props per game (v2)
      - filters vendors to fanduel + draftkings
      - classifies windows (FULL / Q1 / FIRST3MIN / H1 ...)
      - applies FanDuel correction
      - optionally writes to BQ (safe modes)
    """
    client = get_bq_client()
    snapshot_ts = now_iso()

    games = fetch_games_for_date(game_date)
    if not games:
        return {"status": "no_games", "date": game_date, "rows": 0}

    all_rows: List[dict] = []
    games_with_props = 0
    seen_props = 0

    for g in games:
        gid = g.get("id")
        if not gid:
            continue

        try:
            props = fetch_player_props_for_game(int(gid))
        except Exception as e:
            print(f"❌ props pull failed for game_id={gid}: {e}")
            time.sleep(RATE_DELAY_SEC)
            continue

        seen_props += len(props)
        if props:
            games_with_props += 1

        for p in props:
            np = normalize_prop(p, snapshot_ts=snapshot_ts)
            if not np:
                continue
            all_rows.append(np.to_row())

        time.sleep(RATE_DELAY_SEC)

    # -----------------------------
    # DRY RUN summary
    # -----------------------------
    by_vendor: Dict[str, int] = {}
    by_window: Dict[str, int] = {}
    by_market: Dict[str, int] = {}

    for r in all_rows:
        by_vendor[r["vendor"]] = by_vendor.get(r["vendor"], 0) + 1
        by_window[r["market_window"]] = by_window.get(r["market_window"], 0) + 1
        by_market[r["market_key"]] = by_market.get(r["market_key"], 0) + 1

    def topk(d: Dict[str, int], k: int = 12) -> List[Tuple[str, int]]:
        return sorted(d.items(), key=lambda x: x[1], reverse=True)[:k]

    print("\n==================== MASTER PROP INGEST ====================")
    print(f"date: {game_date}")
    print(f"vendors: {VENDORS}")
    print(f"games: {len(games)} | games_with_props: {games_with_props}")
    print(f"raw_props_seen: {seen_props} | normalized_rows: {len(all_rows)}")
    print(f"by_vendor: {by_vendor}")
    print(f"by_window: {by_window}")
    print(f"top_market_keys: {topk(by_market)}")

    # Sample a few rows for sanity (esp window labeling)
    sample = all_rows[:8]
    print("\nSAMPLE ROWS (first 8):")
    print(json.dumps(sample, indent=2)[:2500])

    # -----------------------------
    # WRITE MODES
    # -----------------------------
    if WRITE_MODE == "DRY_RUN":
        return {
            "status": "dry_run",
            "date": game_date,
            "games": len(games),
            "games_with_props": games_with_props,
            "raw_props_seen": seen_props,
            "rows": len(all_rows),
            "by_vendor": by_vendor,
            "by_window": by_window,
        }

    if WRITE_MODE == "STAGING_ONLY":
        bq_overwrite_json(client, TABLE_STAGING, all_rows)
        return {
            "status": "staging_written",
            "date": game_date,
            "rows": len(all_rows),
            "table": fqtn(TABLE_STAGING),
        }

    if WRITE_MODE == "SWAP":
        bq_overwrite_json(client, TABLE_STAGING, all_rows)
        bq_swap_from_staging(client, TABLE_FINAL, TABLE_STAGING)
        return {
            "status": "swapped",
            "date": game_date,
            "rows": len(all_rows),
            "final_table": fqtn(TABLE_FINAL),
        }

    raise ValueError(f"Invalid WRITE_MODE={WRITE_MODE}. Use DRY_RUN | STAGING_ONLY | SWAP.")


# -----------------------------
# CLI ENTRYPOINT
# -----------------------------
if __name__ == "__main__":
    date_arg = os.getenv("GAME_DATE") or today_ny()
    result = ingest_player_props_master(date_arg)
    print("\nRESULT:")
    print(json.dumps(result, indent=2))