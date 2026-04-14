"""
ingest.py - PropFinder data ingestion pipeline
Fetches today's MLB games, lineups, batter hit-data, splits, and pitcher matchup data.
Writes raw data to BigQuery propfinder dataset.

v5 changes:
- fetch_teams() added for all-MLB strikeout K rankings (raw_team_strikeout_rankings)
- raw_pitcher_matchup gains 6 strikeout columns:
    strikeouts, strikeouts_per_9, strikeout_walk_ratio, k_pct, strike_pct, batters_faced
- fetch_teams() called once per run in main() after weather_notes

v4 changes:
- fetch_today_games + fetch_lineup replaced by fetch_upcoming_games (propfinder API)
- fetch_weather_notes added for expert weather commentary
- raw_game_weather table populated with per-game weather + odds data
- batter_team_id stored so model can correctly match batters to pitchers
"""

import asyncio
import csv
import datetime
import io
import logging
import time
from zoneinfo import ZoneInfo

import aiohttp
from google.api_core.retry import Retry
from google.cloud import bigquery

PROJECT = "graphite-flare-477419-h7"
DATASET = "propfinder"
BASE_URL = "https://api.propfinder.app"
MLB_API = "https://statsapi.mlb.com/api/v1"
SLATE_TZ = ZoneInfo("America/New_York")
TODAY = datetime.datetime.now(SLATE_TZ).date()
NOW = datetime.datetime.now(datetime.timezone.utc)
CURRENT_SEASON = TODAY.year
INSERT_CHUNK_SIZE = 250
INSERT_MAX_ATTEMPTS = 5
INSERT_TIMEOUT_SECONDS = 45

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
bq = bigquery.Client(project=PROJECT)


def table(name):
    return f"{PROJECT}.{DATASET}.{name}"


# ── Schemas ───────────────────────────────────────────────────────────────────

RAW_GAME_WEATHER_SCHEMA = [
    bigquery.SchemaField("run_date",          "DATE"),
    bigquery.SchemaField("game_pk",           "INTEGER"),
    bigquery.SchemaField("game_date",         "TIMESTAMP"),
    bigquery.SchemaField("home_team_id",      "INTEGER"),
    bigquery.SchemaField("home_team_name",    "STRING"),
    bigquery.SchemaField("away_team_id",      "INTEGER"),
    bigquery.SchemaField("away_team_name",    "STRING"),
    bigquery.SchemaField("weather_indicator", "STRING"),
    bigquery.SchemaField("game_temp",         "FLOAT"),
    bigquery.SchemaField("wind_speed",        "FLOAT"),
    bigquery.SchemaField("wind_dir",          "INTEGER"),
    bigquery.SchemaField("wind_gust",         "FLOAT"),
    bigquery.SchemaField("precip_prob",       "FLOAT"),
    bigquery.SchemaField("conditions",        "STRING"),
    bigquery.SchemaField("ballpark_name",     "STRING"),
    bigquery.SchemaField("roof_type",         "STRING"),
    bigquery.SchemaField("ballpark_azimuth",  "INTEGER"),
    bigquery.SchemaField("home_moneyline",    "INTEGER"),
    bigquery.SchemaField("away_moneyline",    "INTEGER"),
    bigquery.SchemaField("over_under",        "FLOAT"),
    bigquery.SchemaField("weather_note",      "STRING"),
    bigquery.SchemaField("ingested_at",       "TIMESTAMP"),
]

RAW_HR_PROPS_SCHEMA = [
    bigquery.SchemaField("run_date",           "DATE"),
    bigquery.SchemaField("game_pk",            "INTEGER"),
    bigquery.SchemaField("player_id",          "INTEGER"),
    bigquery.SchemaField("player_name",        "STRING"),
    bigquery.SchemaField("hr_odds_best_price", "INTEGER"),
    bigquery.SchemaField("hr_odds_best_book",  "STRING"),
    bigquery.SchemaField("deep_link_desktop",  "STRING"),
    bigquery.SchemaField("deep_link_ios",      "STRING"),
    bigquery.SchemaField("dk_outcome_code",    "STRING"),
    bigquery.SchemaField("dk_event_id",        "STRING"),
    bigquery.SchemaField("fd_market_id",       "STRING"),
    bigquery.SchemaField("fd_selection_id",    "STRING"),
    bigquery.SchemaField("ingested_at",        "TIMESTAMP"),
]

RAW_K_PROPS_SCHEMA = [
    bigquery.SchemaField("run_date",           "DATE"),
    bigquery.SchemaField("game_pk",            "INTEGER"),
    bigquery.SchemaField("pitcher_id",         "INTEGER"),
    bigquery.SchemaField("pitcher_name",       "STRING"),
    bigquery.SchemaField("team_code",          "STRING"),
    bigquery.SchemaField("opp_team_code",      "STRING"),
    bigquery.SchemaField("line",               "FLOAT"),
    bigquery.SchemaField("over_under",         "STRING"),
    bigquery.SchemaField("best_price",         "INTEGER"),
    bigquery.SchemaField("best_book",          "STRING"),
    bigquery.SchemaField("pf_rating",          "FLOAT"),
    bigquery.SchemaField("hit_rate_l10",       "STRING"),
    bigquery.SchemaField("hit_rate_season",    "STRING"),
    bigquery.SchemaField("hit_rate_vs_team",   "STRING"),
    bigquery.SchemaField("avg_l10",            "FLOAT"),
    bigquery.SchemaField("avg_home_away",      "FLOAT"),
    bigquery.SchemaField("avg_vs_opponent",    "FLOAT"),
    bigquery.SchemaField("streak",             "INTEGER"),
    bigquery.SchemaField("deep_link_desktop",  "STRING"),
    bigquery.SchemaField("deep_link_ios",      "STRING"),
    bigquery.SchemaField("ingested_at",        "TIMESTAMP"),
]

RAW_HIT_PROPS_SCHEMA = [
    bigquery.SchemaField("run_date",           "DATE"),
    bigquery.SchemaField("game_pk",            "INTEGER"),
    bigquery.SchemaField("batter_id",          "INTEGER"),
    bigquery.SchemaField("batter_name",        "STRING"),
    bigquery.SchemaField("team_code",          "STRING"),
    bigquery.SchemaField("opp_team_code",      "STRING"),
    bigquery.SchemaField("position",           "STRING"),
    bigquery.SchemaField("line",               "FLOAT"),
    bigquery.SchemaField("over_under",         "STRING"),
    bigquery.SchemaField("best_price",         "INTEGER"),
    bigquery.SchemaField("best_book",          "STRING"),
    bigquery.SchemaField("pf_rating",          "FLOAT"),
    bigquery.SchemaField("matchup_value",      "FLOAT"),
    bigquery.SchemaField("matchup_label",      "STRING"),
    bigquery.SchemaField("hit_rate_l5",        "STRING"),
    bigquery.SchemaField("hit_rate_l10",       "STRING"),
    bigquery.SchemaField("hit_rate_l20",       "STRING"),
    bigquery.SchemaField("hit_rate_season",    "STRING"),
    bigquery.SchemaField("hit_rate_vs_team",   "STRING"),
    bigquery.SchemaField("hit_rate_last_season","STRING"),
    bigquery.SchemaField("avg_l10",            "FLOAT"),
    bigquery.SchemaField("avg_home_away",      "FLOAT"),
    bigquery.SchemaField("avg_vs_opponent",    "FLOAT"),
    bigquery.SchemaField("streak",             "INTEGER"),
    bigquery.SchemaField("deep_link_desktop",  "STRING"),
    bigquery.SchemaField("deep_link_ios",      "STRING"),
    bigquery.SchemaField("dk_event_id",        "STRING"),
    bigquery.SchemaField("dk_outcome_code",    "STRING"),
    bigquery.SchemaField("fd_market_id",       "STRING"),
    bigquery.SchemaField("fd_selection_id",    "STRING"),
    bigquery.SchemaField("ingested_at",        "TIMESTAMP"),
]

RAW_PITCHER_VS_BATTING_ORDER_SCHEMA = [
    bigquery.SchemaField("run_date",      "DATE"),
    bigquery.SchemaField("game_pk",       "INTEGER"),
    bigquery.SchemaField("pitcher_id",    "INTEGER"),
    bigquery.SchemaField("pitcher_name",  "STRING"),
    bigquery.SchemaField("pitcher_hand",  "STRING"),
    bigquery.SchemaField("opp_team_id",   "INTEGER"),
    bigquery.SchemaField("season",        "INTEGER"),
    bigquery.SchemaField("batting_order", "INTEGER"),
    bigquery.SchemaField("at_bats",       "INTEGER"),
    bigquery.SchemaField("hits",          "INTEGER"),
    bigquery.SchemaField("home_runs",     "INTEGER"),
    bigquery.SchemaField("doubles",       "INTEGER"),
    bigquery.SchemaField("triples",       "INTEGER"),
    bigquery.SchemaField("rbi",           "INTEGER"),
    bigquery.SchemaField("walks",         "INTEGER"),
    bigquery.SchemaField("strike_outs",   "INTEGER"),
    bigquery.SchemaField("avg",           "FLOAT"),
    bigquery.SchemaField("obp",           "FLOAT"),
    bigquery.SchemaField("slg",           "FLOAT"),
    bigquery.SchemaField("ops",           "FLOAT"),
    bigquery.SchemaField("ingested_at",   "TIMESTAMP"),
]

RAW_TEAM_STRIKEOUT_RANKINGS_SCHEMA = [
    bigquery.SchemaField("run_date",    "DATE"),
    bigquery.SchemaField("team_id",     "INTEGER"),
    bigquery.SchemaField("team_code",   "STRING"),
    bigquery.SchemaField("team_name",   "STRING"),
    bigquery.SchemaField("category",    "STRING"),
    bigquery.SchemaField("split",       "STRING"),
    bigquery.SchemaField("rank",        "INTEGER"),
    bigquery.SchemaField("value",       "INTEGER"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]

NEW_HR_PICKS_FIELDS = [
    bigquery.SchemaField("weather_indicator",  "STRING"),
    bigquery.SchemaField("game_temp",          "FLOAT"),
    bigquery.SchemaField("wind_speed",         "FLOAT"),
    bigquery.SchemaField("wind_dir",           "INTEGER"),
    bigquery.SchemaField("precip_prob",        "FLOAT"),
    bigquery.SchemaField("ballpark_name",      "STRING"),
    bigquery.SchemaField("roof_type",          "STRING"),
    bigquery.SchemaField("weather_note",       "STRING"),
    bigquery.SchemaField("home_moneyline",     "INTEGER"),
    bigquery.SchemaField("away_moneyline",     "INTEGER"),
    bigquery.SchemaField("over_under",         "FLOAT"),
    bigquery.SchemaField("hr_odds_best_price", "INTEGER"),
    bigquery.SchemaField("hr_odds_best_book",  "STRING"),
    bigquery.SchemaField("deep_link_desktop",  "STRING"),
    bigquery.SchemaField("deep_link_ios",      "STRING"),
    bigquery.SchemaField("dk_outcome_code",    "STRING"),
    bigquery.SchemaField("dk_event_id",        "STRING"),
    bigquery.SchemaField("fd_market_id",       "STRING"),
    bigquery.SchemaField("fd_selection_id",    "STRING"),
]

RAW_STATCAST_BATTER_PITCH_SCHEMA = [
    bigquery.SchemaField("run_date",          "DATE"),
    bigquery.SchemaField("batter_id",         "INTEGER"),
    bigquery.SchemaField("batter_name",       "STRING"),
    bigquery.SchemaField("game_year",         "INTEGER"),
    bigquery.SchemaField("pitch_type",        "STRING"),
    bigquery.SchemaField("pitch_name",        "STRING"),
    bigquery.SchemaField("p_throws",          "STRING"),
    bigquery.SchemaField("pa",                "INTEGER"),
    bigquery.SchemaField("ab",                "INTEGER"),
    bigquery.SchemaField("hits",              "INTEGER"),
    bigquery.SchemaField("hr",                "INTEGER"),
    bigquery.SchemaField("doubles",           "INTEGER"),
    bigquery.SchemaField("triples",           "INTEGER"),
    bigquery.SchemaField("so",                "INTEGER"),
    bigquery.SchemaField("bb",                "INTEGER"),
    bigquery.SchemaField("hbp",               "INTEGER"),
    bigquery.SchemaField("avg",               "FLOAT"),
    bigquery.SchemaField("obp",               "FLOAT"),
    bigquery.SchemaField("slg",               "FLOAT"),
    bigquery.SchemaField("iso",               "FLOAT"),
    bigquery.SchemaField("woba",              "FLOAT"),
    bigquery.SchemaField("k_pct",             "FLOAT"),
    bigquery.SchemaField("bb_pct",            "FLOAT"),
    bigquery.SchemaField("avg_ev",            "FLOAT"),
    bigquery.SchemaField("barrel_pct",        "FLOAT"),
    bigquery.SchemaField("hh_pct",            "FLOAT"),
    bigquery.SchemaField("ingested_at",       "TIMESTAMP"),
]

# New columns added idempotently to raw_pitcher_matchup
NEW_PITCHER_MATCHUP_FIELDS = [
    bigquery.SchemaField("strikeouts",           "INTEGER"),
    bigquery.SchemaField("strikeouts_per_9",     "FLOAT"),
    bigquery.SchemaField("strikeout_walk_ratio", "FLOAT"),
    bigquery.SchemaField("k_pct",                "FLOAT"),
    bigquery.SchemaField("strike_pct",           "FLOAT"),
    bigquery.SchemaField("batters_faced",        "INTEGER"),
]


# ── Table management ──────────────────────────────────────────────────────────

def ensure_tables():
    """Create tables if missing; add new columns idempotently."""

    # raw_game_weather
    try:
        bq.create_table(
            bigquery.Table(table("raw_game_weather"), schema=RAW_GAME_WEATHER_SCHEMA),
            exists_ok=True,
        )
        log.info("raw_game_weather table ready")
    except Exception as exc:
        log.error("Failed to create raw_game_weather: %s", exc)
        raise

    # raw_game_weather — add any missing columns
    try:
        gw_table = bq.get_table(table("raw_game_weather"))
        existing = {f.name for f in gw_table.schema}
        gw_new = [f for f in RAW_GAME_WEATHER_SCHEMA if f.name not in existing]
        if gw_new:
            gw_table.schema = list(gw_table.schema) + gw_new
            bq.update_table(gw_table, ["schema"])
            log.info("Added columns to raw_game_weather: %s", [f.name for f in gw_new])
    except Exception as exc:
        log.warning("Could not update raw_game_weather schema: %s", exc)

    # raw_hr_props
    try:
        bq.create_table(
            bigquery.Table(table("raw_hr_props"), schema=RAW_HR_PROPS_SCHEMA),
            exists_ok=True,
        )
        log.info("raw_hr_props table ready")
    except Exception as exc:
        log.warning("Could not create raw_hr_props: %s", exc)

    # raw_pitcher_vs_batting_order
    try:
        bq.create_table(
            bigquery.Table(
                table("raw_pitcher_vs_batting_order"),
                schema=RAW_PITCHER_VS_BATTING_ORDER_SCHEMA,
            ),
            exists_ok=True,
        )
        log.info("raw_pitcher_vs_batting_order table ready")
    except Exception as exc:
        log.warning("Could not create raw_pitcher_vs_batting_order: %s", exc)

    # raw_k_props
    try:
        bq.create_table(
            bigquery.Table(table("raw_k_props"), schema=RAW_K_PROPS_SCHEMA),
            exists_ok=True,
        )
        log.info("raw_k_props table ready")
    except Exception as exc:
        log.warning("Could not create raw_k_props: %s", exc)

    # raw_hit_props
    try:
        bq.create_table(
            bigquery.Table(table("raw_hit_props"), schema=RAW_HIT_PROPS_SCHEMA),
            exists_ok=True,
        )
        log.info("raw_hit_props table ready")
    except Exception as exc:
        log.warning("Could not create raw_hit_props: %s", exc)

    # raw_team_strikeout_rankings
    try:
        bq.create_table(
            bigquery.Table(
                table("raw_team_strikeout_rankings"),
                schema=RAW_TEAM_STRIKEOUT_RANKINGS_SCHEMA,
            ),
            exists_ok=True,
        )
        log.info("raw_team_strikeout_rankings table ready")
    except Exception as exc:
        log.error("Failed to create raw_team_strikeout_rankings: %s", exc)
        raise

    # hr_picks_daily — add new columns idempotently
    try:
        hr_table = bq.get_table(table("hr_picks_daily"))
        existing = {f.name for f in hr_table.schema}
        to_add = [f for f in NEW_HR_PICKS_FIELDS if f.name not in existing]
        if to_add:
            hr_table.schema = list(hr_table.schema) + to_add
            bq.update_table(hr_table, ["schema"])
            log.info("Added %s columns to hr_picks_daily: %s", len(to_add), [f.name for f in to_add])
    except Exception as exc:
        log.warning("Could not update hr_picks_daily schema: %s", exc)

    # raw_statcast_batter_pitch_stats
    try:
        bq.create_table(
            bigquery.Table(
                table("raw_statcast_batter_pitch_stats"),
                schema=RAW_STATCAST_BATTER_PITCH_SCHEMA,
            ),
            exists_ok=True,
        )
        log.info("raw_statcast_batter_pitch_stats table ready")
    except Exception as exc:
        log.warning("Could not create raw_statcast_batter_pitch_stats: %s", exc)

    # raw_pitcher_matchup — add strikeout columns idempotently
    try:
        pm_table = bq.get_table(table("raw_pitcher_matchup"))
        existing = {f.name for f in pm_table.schema}
        to_add = [f for f in NEW_PITCHER_MATCHUP_FIELDS if f.name not in existing]
        if to_add:
            pm_table.schema = list(pm_table.schema) + to_add
            bq.update_table(pm_table, ["schema"])
            log.info(
                "Added %s columns to raw_pitcher_matchup: %s",
                len(to_add),
                [f.name for f in to_add],
            )
    except Exception as exc:
        log.warning("Could not update raw_pitcher_matchup schema: %s", exc)


# ── HTTP helpers ──────────────────────────────────────────────────────────────

async def get(session, url, params=None):
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with session.get(url, params=params, timeout=timeout) as response:
            if response.status == 200:
                return await response.json(content_type=None)
            log.warning("HTTP %s %s params=%s", response.status, url, params)
            return None
    except Exception as exc:
        log.error("Request failed %s: %s", url, exc)
        return None


def sf(value):
    """Safe float."""
    try:
        s = str(value or "0")
        return float("0" + s if s.startswith(".") else s)
    except (ValueError, TypeError):
        return None


def si(value):
    """Safe int."""
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def unique_ints(values):
    out = []
    seen = set()
    for value in values:
        parsed = si(value)
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        out.append(parsed)
    return out


def extract_player_id(player):
    if isinstance(player, dict):
        return si(player.get("id") or player.get("playerId"))
    return si(player)


def extract_position_abbr(player):
    if not isinstance(player, dict):
        return ""
    pos = player.get("position") or {}
    person = player.get("person") or {}
    primary = person.get("primaryPosition") or {}
    abbr = pos.get("abbreviation") or primary.get("abbreviation") or ""
    return str(abbr).upper()


# ── BQ insert ─────────────────────────────────────────────────────────────────

def bq_insert(table_name, rows):
    if not rows:
        log.info("No rows for %s", table_name)
        return
    table_ref = table(table_name)
    total = len(rows)
    inserted = 0
    chunks = (total + INSERT_CHUNK_SIZE - 1) // INSERT_CHUNK_SIZE
    retry_policy = Retry(deadline=INSERT_TIMEOUT_SECONDS)

    for chunk_idx, start in enumerate(range(0, total, INSERT_CHUNK_SIZE), start=1):
        chunk = rows[start : start + INSERT_CHUNK_SIZE]
        chunk_ok = False
        last_error = None

        for attempt in range(1, INSERT_MAX_ATTEMPTS + 1):
            try:
                errors = bq.insert_rows_json(
                    table_ref,
                    chunk,
                    retry=retry_policy,
                    timeout=INSERT_TIMEOUT_SECONDS,
                )
                if errors:
                    log.error(
                        "BQ insert row errors %s chunk=%s/%s: %s",
                        table_name,
                        chunk_idx,
                        chunks,
                        errors[:3],
                    )
                    raise RuntimeError(
                        f"BigQuery rejected rows for {table_name} chunk {chunk_idx}/{chunks}"
                    )
                inserted += len(chunk)
                chunk_ok = True
                break
            except Exception as exc:
                last_error = exc
                if attempt < INSERT_MAX_ATTEMPTS:
                    sleep_s = 2 ** (attempt - 1)
                    log.warning(
                        "BQ insert retry %s chunk=%s/%s attempt=%s/%s after error: %s",
                        table_name,
                        chunk_idx,
                        chunks,
                        attempt,
                        INSERT_MAX_ATTEMPTS,
                        exc,
                    )
                    time.sleep(sleep_s)
                else:
                    break

        if not chunk_ok:
            raise RuntimeError(
                f"BQ insert failed for {table_name} chunk {chunk_idx}/{chunks}: {last_error}"
            ) from last_error

    log.info("Inserted %s rows -> %s (%s chunks)", inserted, table_name, chunks)


# ── Fetch functions ───────────────────────────────────────────────────────────

def _find_game_weather(weather_data, game_epoch):
    """Return the weatherData entry whose dateTimeEpoch is closest to game_epoch."""
    if not weather_data:
        return {}
    return min(weather_data, key=lambda w: abs((w.get("dateTimeEpoch") or 0) - game_epoch))


async def fetch_upcoming_games(session):
    """
    Calls /mlb/upcoming-games, filters to TODAY, extracts game info
    including batting orders, weather, and odds.
    """
    data = await get(session, f"{BASE_URL}/mlb/upcoming-games")
    if not data or not isinstance(data, list):
        return []

    games = []
    for item in data:
        game_date_str = item.get("gameDate", "")
        if not game_date_str:
            continue
        try:
            game_dt = datetime.datetime.fromisoformat(game_date_str.replace("Z", "+00:00"))
        except ValueError:
            continue

        game_local_date = (
            game_dt.astimezone(SLATE_TZ).date()
            if game_dt.tzinfo
            else game_dt.date()
        )
        if game_local_date != TODAY:
            continue

        game_pk = si(item.get("id"))
        if game_pk is None:
            continue

        game_epoch = int(game_dt.timestamp())

        home_team    = item.get("homeTeam") or {}
        visitor_team = item.get("visitorTeam") or {}
        ballpark     = item.get("ballpark") or {}
        weather_data = item.get("weatherData") or []
        game_weather = _find_game_weather(weather_data, game_epoch)

        home_order_str    = item.get("homeBattingOrder") or ""
        visitor_order_str = item.get("visitorBattingOrder") or ""
        home_batting_order = [
            int(x) for x in home_order_str.split(",") if x.strip().isdigit()
        ]
        away_batting_order = [
            int(x) for x in visitor_order_str.split(",") if x.strip().isdigit()
        ]

        home_ml_str = item.get("homeTeamOdds")
        away_ml_str = item.get("visitorTeamOdds")

        games.append({
            "game_pk":            game_pk,
            "game_date":          game_date_str,
            "game_epoch":         game_epoch,
            "home_team_id":       si(home_team.get("id")),
            "home_team_name":     home_team.get("fullName", ""),
            "home_team_code":     home_team.get("code", ""),
            "away_team_id":       si(visitor_team.get("id")),
            "away_team_name":     visitor_team.get("fullName", ""),
            "away_team_code":     visitor_team.get("code", ""),
            "home_pitcher_id":    si(item.get("homePitcherId")),
            "away_pitcher_id":    si(item.get("visitorPitcherId")),
            "home_batting_order": home_batting_order,
            "away_batting_order": away_batting_order,
            "weather_indicator":  item.get("weatherIndicator", ""),
            "home_moneyline":     si(home_ml_str) if home_ml_str else None,
            "away_moneyline":     si(away_ml_str) if away_ml_str else None,
            "over_under":         sf(item.get("gameRunLine")),
            "ballpark_name":      ballpark.get("name", ""),
            "roof_type":          ballpark.get("roofType", ""),
            "ballpark_azimuth":   si(ballpark.get("azimuthAngle")),
            "game_temp":          sf(game_weather.get("temp")),
            "wind_speed":         sf(game_weather.get("windSpeed")),
            "wind_dir":           si(game_weather.get("windDir")),
            "wind_gust":          sf(game_weather.get("windGust")),
            "precip_prob":        sf(game_weather.get("precipProb")),
            "conditions":         game_weather.get("conditions", ""),
        })

    log.info("Found %s games for %s from upcoming-games endpoint", len(games), TODAY)
    return games


async def fetch_weather_notes(session):
    """Calls /mlb/weather-notes. Returns dict keyed by gameId."""
    data = await get(session, f"{BASE_URL}/mlb/weather-notes")
    if not data or not isinstance(data, list):
        return {}

    notes = {}
    for item in data:
        game_id = si(item.get("gameId"))
        if game_id is None:
            continue
        notes[game_id] = {
            "content": item.get("content", ""),
            "author":  item.get("authorName", ""),
        }
    log.info("Fetched %s weather notes", len(notes))
    return notes


async def fetch_teams(session):
    """
    Calls /mlb/teams — returns all 30 MLB teams with strikeout K-count
    rankings across splits (Season, L15 Days, L30 Days, Home, Away,
    vs LHP, vs RHP).

    Returns list of BQ-ready rows for raw_team_strikeout_rankings.
    """
    data = await get(session, f"{BASE_URL}/mlb/teams")
    if not data or not isinstance(data, list):
        log.warning("fetch_teams: no data returned")
        return []

    rows = []
    for team in data:
        team_id   = si(team.get("id"))
        team_code = team.get("code", "")
        team_name = team.get("fullName", "")

        for ranking in team.get("rankings", []):
            rows.append({
                "run_date":    TODAY.isoformat(),
                "team_id":     team_id,
                "team_code":   team_code,
                "team_name":   team_name,
                "category":    ranking.get("category", "strikeouts"),
                "split":       ranking.get("split", ""),
                "rank":        si(ranking.get("rank")),
                "value":       si(ranking.get("value")),
                "ingested_at": NOW.isoformat(),
            })

    log.info(
        "fetch_teams: %s ranking rows across %s teams",
        len(rows),
        len(data),
    )
    return rows


def _parse_dk_link(url):
    """Extract (dk_event_id, dk_outcome_code) from a DraftKings desktop URL."""
    import re
    m = re.search(r"/event/(\d+)\?outcomes=([^&\s]+)", url or "")
    if m:
        return m.group(1), m.group(2)
    return None, None


def _parse_fd_link(url):
    """Extract (fd_market_id, fd_selection_id) from a FanDuel desktop URL."""
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(url or "")
    qs = parse_qs(parsed.query)
    return qs.get("marketId", [None])[0], qs.get("selectionId", [None])[0]


async def fetch_player_names(session, player_ids):
    """Batch-fetch player fullNames from MLB Stats API by ID list."""
    if not player_ids:
        return {}
    ids_str = ",".join(str(i) for i in player_ids)
    data = await get(
        session,
        f"{MLB_API}/people",
        params={"personIds": ids_str, "fields": "people,id,fullName"},
    )
    if not data or not isinstance(data, dict):
        return {}
    return {
        int(p["id"]): p.get("fullName", "")
        for p in data.get("people", [])
        if p.get("id")
    }


async def fetch_props(session, game_pk):
    """
    Fetch HR prop odds for a game from /mlb/props.
    Returns dict keyed by player_id with odds + deep link fields.
    """
    data = await get(session, f"{BASE_URL}/mlb/props", params={"gameId": game_pk})
    if not data or not isinstance(data, list):
        return {}

    props = {}
    for item in data:
        player_id = si(item.get("playerId"))
        if player_id is None:
            continue
        best = item.get("bestMarket") or {}
        desktop_link = best.get("deepLinkDesktop") or ""
        ios_link     = best.get("deepLinkIos") or best.get("deepLinkMobile") or ""

        dk_event_id, dk_outcome_code = _parse_dk_link(desktop_link)
        fd_market_id, fd_selection_id = _parse_fd_link(desktop_link)

        props[player_id] = {
            "player_name":        item.get("playerName") or "",
            "hr_odds_best_price": si(best.get("price")),
            "hr_odds_best_book":  best.get("bookmaker") or best.get("book") or "",
            "deep_link_desktop":  desktop_link,
            "deep_link_ios":      ios_link,
            "dk_outcome_code":    dk_outcome_code or "",
            "dk_event_id":        dk_event_id or "",
            "fd_market_id":       fd_market_id or "",
            "fd_selection_id":    fd_selection_id or "",
        }

    log.info("Fetched HR props for %s players in game %s", len(props), game_pk)
    return props


async def fetch_k_props(session, game_pk):
    """
    Fetch standard (non-alt) pitcher strikeout over/under props for a game.
    Returns list of BQ-ready rows for raw_k_props.
    """
    data = await get(session, f"{BASE_URL}/mlb/props", params={"gameId": game_pk})
    if not data or not isinstance(data, list):
        return []

    rows = []
    for item in data:
        if item.get("category") != "pitching_strikeouts":
            continue
        if item.get("isAlternate") is not None:
            continue
        ou_side = (item.get("overUnder") or "").lower()
        if ou_side not in ("over", "under"):
            continue

        best = item.get("bestMarket") or {}
        rows.append({
            "run_date":          TODAY.isoformat(),
            "game_pk":           game_pk,
            "pitcher_id":        si(item.get("playerId")),
            "pitcher_name":      item.get("name", ""),
            "team_code":         item.get("teamCode", ""),
            "opp_team_code":     item.get("opposingTeamCode", ""),
            "line":              sf(item.get("line")),
            "over_under":        ou_side,
            "best_price":        si(best.get("price")),
            "best_book":         best.get("sportsbook", ""),
            "pf_rating":         sf(item.get("pfRating")),
            "hit_rate_l10":      item.get("hitRateL10", ""),
            "hit_rate_season":   item.get("hitRateSeason", ""),
            "hit_rate_vs_team":  item.get("hitRateVsTeam", ""),
            "avg_l10":           sf(item.get("avgL10")),
            "avg_home_away":     sf(item.get("avgHomeAway")),
            "avg_vs_opponent":   sf(item.get("avgVsOpponent")),
            "streak":            si(item.get("streak")),
            "deep_link_desktop": best.get("deepLinkDesktop", ""),
            "deep_link_ios":     best.get("deepLinkIos", ""),
            "ingested_at":       NOW.isoformat(),
        })

    log.info("Fetched K props for %s pitchers in game %s", len(rows), game_pk)
    return rows


async def fetch_hit_props(session, game_pk):
    """
    Fetch standard (non-alt) batter hits over/under props for a game.
    Returns list of BQ-ready rows for raw_hit_props.
    """
    import re
    from urllib.parse import parse_qs, urlsplit

    data = await get(session, f"{BASE_URL}/mlb/props", params={"gameId": game_pk})
    if not data or not isinstance(data, list):
        return []

    rows = []
    for item in data:
        if item.get("category") != "hits":
            continue
        if item.get("isAlternate") is not None:
            continue
        ou_side = (item.get("overUnder") or "").lower()
        if ou_side not in ("over", "under"):
            continue

        best = item.get("bestMarket") or {}
        markets = item.get("markets") if isinstance(item.get("markets"), list) else []
        if best:
            markets = list(markets) + [best]

        # Parse DK/FD deep links from all markets
        dk_event_id = None
        dk_outcome_code = None
        fd_market_id = None
        fd_selection_id = None

        for mkt in markets:
            sb = (mkt.get("sportsbook") or "").lower()
            desktop = (mkt.get("deepLinkDesktop") or "").strip()
            if not desktop:
                continue

            if "draftkings" in sb and not dk_event_id:
                split = urlsplit(desktop)
                match = re.search(r"/event/([^/?#]+)", split.path)
                dk_event_id = match.group(1) if match else None
                for part in (split.query or "").split("&"):
                    if part.startswith("outcomes="):
                        dk_outcome_code = part.split("=", 1)[1].strip() or None
                        break

            if "fanduel" in sb and not fd_market_id:
                split = urlsplit(desktop)
                qs = parse_qs(split.query, keep_blank_values=False)
                fd_market_id = (qs.get("marketId") or qs.get("marketId[0]") or qs.get("marketId[]") or [None])[0]
                fd_selection_id = (qs.get("selectionId") or qs.get("selectionId[0]") or qs.get("selectionId[]") or [None])[0]

        rows.append({
            "run_date":             TODAY.isoformat(),
            "game_pk":              game_pk,
            "batter_id":            si(item.get("playerId")),
            "batter_name":          item.get("name", ""),
            "team_code":            item.get("teamCode", ""),
            "opp_team_code":        item.get("opposingTeamCode", ""),
            "position":             item.get("position", ""),
            "line":                 sf(item.get("line")),
            "over_under":           ou_side,
            "best_price":           si(best.get("price")),
            "best_book":            best.get("sportsbook", ""),
            "pf_rating":            sf(item.get("pfRating")),
            "matchup_value":        sf(item.get("matchupValue")),
            "matchup_label":        item.get("matchupLabel", ""),
            "hit_rate_l5":          item.get("hitRateL5", ""),
            "hit_rate_l10":         item.get("hitRateL10", ""),
            "hit_rate_l20":         item.get("hitRateL20", ""),
            "hit_rate_season":      item.get("hitRateSeason", ""),
            "hit_rate_vs_team":     item.get("hitRateVsTeam", ""),
            "hit_rate_last_season": item.get("hitRateLastSeason", ""),
            "avg_l10":              sf(item.get("avgL10")),
            "avg_home_away":        sf(item.get("avgHomeAway")),
            "avg_vs_opponent":      sf(item.get("avgVsOpponent")),
            "streak":               si(item.get("streak")),
            "deep_link_desktop":    best.get("deepLinkDesktop", ""),
            "deep_link_ios":        best.get("deepLinkIos", ""),
            "dk_event_id":          dk_event_id,
            "dk_outcome_code":      dk_outcome_code,
            "fd_market_id":         fd_market_id,
            "fd_selection_id":      fd_selection_id,
            "ingested_at":          NOW.isoformat(),
        })

    log.info("Fetched hit props for %s batters in game %s", len(rows), game_pk)
    return rows


async def fetch_batting_order_splits(session, pitcher_id, pitcher_name, pitcher_hand, opp_team_id, game_pk):
    """
    Fetch pitcher stats vs each batting order position (1-9) from MLB Stats API.
    Uses sitCodes b1-b9 for the current season.
    """
    data = await get(
        session,
        f"{MLB_API}/people/{pitcher_id}/stats",
        params={
            "stats": "statSplits",
            "group": "pitching",
            "sitCodes": "b1,b2,b3,b4,b5,b6,b7,b8,b9",
            "season": CURRENT_SEASON,
        },
    )
    if not data or not isinstance(data, dict):
        return []

    rows = []
    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            sp = split.get("split", {})
            st = split.get("stat", {})
            code = sp.get("code", "")
            if not code.startswith("b"):
                continue
            order_num = si(code[1:])
            if order_num is None or order_num < 1 or order_num > 9:
                continue

            rows.append({
                "run_date":      TODAY.isoformat(),
                "game_pk":       game_pk,
                "pitcher_id":    pitcher_id,
                "pitcher_name":  pitcher_name,
                "pitcher_hand":  pitcher_hand,
                "opp_team_id":   opp_team_id,
                "season":        CURRENT_SEASON,
                "batting_order": order_num,
                "at_bats":       si(st.get("atBats")),
                "hits":          si(st.get("hits")),
                "home_runs":     si(st.get("homeRuns")),
                "doubles":       si(st.get("doubles")),
                "triples":       si(st.get("triples")),
                "rbi":           si(st.get("rbi")),
                "walks":         si(st.get("baseOnBalls")),
                "strike_outs":   si(st.get("strikeOuts")),
                "avg":           sf(st.get("avg")),
                "obp":           sf(st.get("obp")),
                "slg":           sf(st.get("slg")),
                "ops":           sf(st.get("ops")),
                "ingested_at":   NOW.isoformat(),
            })

    log.info("fetch_batting_order_splits: %s rows for pitcher %s", len(rows), pitcher_name)
    return rows


async def fetch_team_roster(session, team_id):
    data = await get(
        session,
        f"{MLB_API}/teams/{team_id}/roster",
        params={"rosterType": "active", "hydrate": "person"},
    )
    if not data or not isinstance(data, dict):
        return []

    roster = data.get("roster", [])
    hitter_ids = []
    for player in roster:
        player_id = extract_player_id((player or {}).get("person") or player)
        position  = extract_position_abbr(player or {})
        if player_id is None or position == "P":
            continue
        hitter_ids.append(player_id)
    return unique_ints(hitter_ids)


async def fetch_hit_data(session, batter_id, game_pk, batter_team_id):
    data = await get(
        session,
        f"{BASE_URL}/mlb/hit-data",
        params={"playerId": batter_id, "group": "hitting"},
    )
    if not data or not isinstance(data, list):
        return []

    rows = []
    for event in data:
        launch_speed = sf(event.get("launchSpeed"))
        if launch_speed is None:
            continue

        rows.append({
            "run_date":       TODAY.isoformat(),
            "game_pk":        game_pk,
            "batter_id":      batter_id,
            "batter_team_id": batter_team_id,
            "batter_name":    event.get("batterName", ""),
            "bat_side":       event.get("batSide", ""),
            "pitcher_id":     si(event.get("pitcherId")),
            "pitcher_name":   event.get("pitcherName", ""),
            "pitch_hand":     event.get("pitchHand", ""),
            "pitch_type":     event.get("pitchType", ""),
            "result":         event.get("result", ""),
            "launch_speed":   launch_speed,
            "launch_angle":   sf(event.get("launchAngle")),
            "total_distance": sf(event.get("totalDistance")),
            "trajectory":     event.get("trajectory", ""),
            "is_barrel":      bool(event.get("isBarrel")),
            "hr_in_n_parks":  si(event.get("hrInNParks", 0)),
            "event_date":     event.get("date", "")[:10],
            "season":         si(event.get("season")),
            "ingested_at":    NOW.isoformat(),
        })
    return rows


SPLITS_WANTED = {"vl", "vr", "h", "a", "r", "g", "t", "preas", "posas"}


def parse_split_float(value):
    parsed = sf(value)
    return parsed if parsed is not None else 0.0


async def fetch_splits(session, batter_id, batter_name):
    data = await get(
        session,
        f"{BASE_URL}/mlb/splits",
        params={"playerId": batter_id, "group": "hitting"},
    )
    if not data or not isinstance(data, list):
        return []

    rows = []
    for split_row in data:
        code = split_row.get("splitCode", "")
        if code not in SPLITS_WANTED:
            continue

        stat      = split_row.get("stat", {})
        at_bats   = si(stat.get("atBats", 0)) or 0
        home_runs = si(stat.get("homeRuns", 0)) or 0
        doubles   = si(stat.get("doubles", 0)) or 0
        triples   = si(stat.get("triples", 0)) or 0

        rows.append({
            "run_date":    TODAY.isoformat(),
            "batter_id":   batter_id,
            "batter_name": batter_name,
            "split_code":  code,
            "split_name":  split_row.get("splitName", ""),
            "season":      str(split_row.get("season", "")),
            "avg":         parse_split_float(stat.get("avg")),
            "obp":         parse_split_float(stat.get("obp")),
            "slg":         parse_split_float(stat.get("slg")),
            "ops":         parse_split_float(stat.get("ops")),
            "home_runs":   home_runs,
            "at_bats":     at_bats,
            "hits":        si(stat.get("hits", 0)),
            "doubles":     doubles,
            "triples":     triples,
            "strike_outs": si(stat.get("strikeOuts", 0)),
            "ingested_at": NOW.isoformat(),
        })
    return rows


PROPFINDER_PITCHLOG_URL = f"{BASE_URL}/mlb/pitchlog"


async def fetch_batter_pitch_stats(session, batter_ids):
    """Fetch per-pitch-type stats for batters from PropFinder's pitchlog endpoint."""
    if not batter_ids:
        return []

    ids_param = ",".join(str(bid) for bid in batter_ids)
    data = await get(session, PROPFINDER_PITCHLOG_URL, params={"playerIds": ids_param})
    if not data or not isinstance(data, list):
        return []

    out = []
    for entry in data:
        season_val = si(entry.get("season"))
        if season_val is None or season_val < 2025:
            continue

        pa = si(entry.get("plateAppearances")) or 0
        if pa == 0:
            continue

        pitch_type = entry.get("pitchCode", "")
        pitch_name = entry.get("pitchName", "")
        p_throws = (entry.get("type") or "").strip()  # "LHP" or "RHP"
        # Normalize hand to single char
        hand = "L" if p_throws == "LHP" else "R" if p_throws == "RHP" else p_throws

        hits = si(entry.get("hits")) or 0
        hr = si(entry.get("homeRuns")) or 0
        singles = si(entry.get("singles")) or 0
        doubles = si(entry.get("doubles")) or 0
        triples = si(entry.get("triples")) or 0
        so = si(entry.get("strikeOuts")) or 0
        bb = si(entry.get("walks")) or 0
        hbp = si(entry.get("hbp")) or 0
        sac_fly = si(entry.get("sacFly")) or 0

        ab = pa - bb - hbp - sac_fly
        avg = sf(entry.get("battingAverage"))
        slg = sf(entry.get("slg"))
        woba = sf(entry.get("wOBA"))
        k_pct_raw = sf(entry.get("kPercent"))
        k_pct = round(k_pct_raw * 100, 1) if k_pct_raw is not None else None
        bb_pct = round((bb / pa) * 100, 1) if pa > 0 else None
        iso = round(slg - avg, 3) if slg is not None and avg is not None else None
        obp_denom = ab + bb + hbp + sac_fly
        obp = round((hits + bb + hbp) / obp_denom, 3) if obp_denom > 0 else None

        out.append({
            "run_date":    TODAY.isoformat(),
            "batter_id":   si(entry.get("playerId")),
            "batter_name": "",
            "game_year":   season_val,
            "pitch_type":  pitch_type,
            "pitch_name":  pitch_name,
            "p_throws":    hand,
            "pa":          pa,
            "ab":          ab,
            "hits":        hits,
            "hr":          hr,
            "doubles":     doubles,
            "triples":     triples,
            "so":          so,
            "bb":          bb,
            "hbp":         hbp,
            "avg":         avg,
            "obp":         obp,
            "slg":         slg,
            "iso":         iso,
            "woba":        woba,
            "k_pct":       k_pct,
            "bb_pct":      bb_pct,
            "avg_ev":      None,
            "barrel_pct":  None,
            "hh_pct":      None,
            "ingested_at": NOW.isoformat(),
        })

    log.info("fetch_batter_pitch_stats: %s batters → %s pitch-type rows", len(batter_ids), len(out))
    return out


def pct_from_metric(value):
    raw = sf(value) or 0.0
    return raw * 100 if raw <= 1 else raw


async def fetch_pitcher_matchup(session, pitcher_id, opp_team_id, game_pk, pitcher_name):
    data = await get(
        session,
        f"{BASE_URL}/mlb/pitcher-matchup",
        params={"pitcherId": pitcher_id, "opposingTeamId": opp_team_id},
    )
    if not data or not isinstance(data, dict):
        return {"splits": [], "pitch_log": []}

    pitcher_hand = data.get("pitchingType", "")

    metric_map = {}
    for metric in data.get("metricAverages", []):
        split_key = metric.get("split", "Season")
        metric_map[split_key] = metric

    split_rows = []
    for split_row in data.get("splits", []):
        if si(split_row.get("season")) != CURRENT_SEASON:
            continue

        split_key = split_row.get("split", "Season")
        metrics   = metric_map.get(split_key, metric_map.get("Season", {}))

        barrel_pct   = pct_from_metric(metrics.get("seasonBarrel"))
        hard_hit_pct = pct_from_metric(metrics.get("seasonHardHitPercentage"))
        fb_pct       = pct_from_metric(metrics.get("flyballPercentage"))

        hr_n      = si(split_row.get("homeRuns")) or 0
        air_outs  = si(split_row.get("airOuts")) or 0
        hr_fb_pct = round((hr_n / air_outs) * 100, 2) if air_outs > 0 else 0.0

        # Strikeout fields
        batters_faced = si(split_row.get("battersFaced")) or 0
        strikeouts    = si(split_row.get("strikeOuts")) or 0
        k_pct         = round((strikeouts / batters_faced) * 100, 4) if batters_faced > 0 else 0.0

        split_rows.append({
            "run_date":             TODAY.isoformat(),
            "game_pk":              game_pk,
            "pitcher_id":           pitcher_id,
            "pitcher_name":         pitcher_name,
            "pitcher_hand":         pitcher_hand,
            "opp_team_id":          opp_team_id,
            "split":                split_key,
            "ip":                   sf(split_row.get("ip")),
            "home_runs":            hr_n,
            "hr_per_9":             sf(split_row.get("homeRunsPer9Inn")),
            "barrel_pct":           round(barrel_pct, 4),
            "hard_hit_pct":         round(hard_hit_pct, 4),
            "fb_pct":               round(fb_pct, 4),
            "hr_fb_pct":            hr_fb_pct,
            "whip":                 sf(split_row.get("whip")),
            "woba":                 sf(split_row.get("woba")),
            # strikeout fields
            "strikeouts":           strikeouts,
            "strikeouts_per_9":     sf(split_row.get("strikeoutsPer9Inn")),
            "strikeout_walk_ratio": sf(split_row.get("strikeoutWalkRatio")),
            "k_pct":                k_pct,
            "strike_pct":           sf(split_row.get("strikePercentage")),
            "batters_faced":        batters_faced,
            "ingested_at":          NOW.isoformat(),
        })

    pitch_log_rows = []
    for pitch in data.get("pitchLog", []):
        pitch_season = si(pitch.get("season"))
        if pitch_season is None or pitch_season < 2025:
            continue

        pitch_log_rows.append({
            "run_date":   TODAY.isoformat(),
            "game_pk":    game_pk,
            "pitcher_id": pitcher_id,
            "batter_hand":pitch.get("type", ""),
            "pitch_code": pitch.get("pitchCode", ""),
            "pitch_name": pitch.get("pitchName", ""),
            "season":     pitch_season,
            "count":      si(pitch.get("count")),
            "percentage": sf(pitch.get("percentage")),
            "home_runs":  si(pitch.get("homeRuns")),
            "woba":       sf(pitch.get("wOBA")),
            "slg":        sf(pitch.get("slg")),
            "iso":        sf(pitch.get("iso")),
            "whiff":      sf(pitch.get("whiff")),
            "k_percent":  sf(pitch.get("kPercent")),
            "ingested_at":NOW.isoformat(),
        })

    return {"splits": split_rows, "pitch_log": pitch_log_rows}


def load_recent_team_batter_rank(team_ids, lookback_days=365):
    team_ids = unique_ints(team_ids)
    if not team_ids:
        return {}

    try:
        sql = f"""
        SELECT
          batter_team_id,
          batter_id,
          COUNT(*) AS events,
          MAX(event_date) AS last_event_date
        FROM `{PROJECT}.{DATASET}.raw_hit_data`
        WHERE batter_team_id IN UNNEST(@team_ids)
          AND run_date >= DATE_SUB(@run_date, INTERVAL @lookback_days DAY)
        GROUP BY batter_team_id, batter_id
        ORDER BY batter_team_id, events DESC, last_event_date DESC, batter_id DESC
        """
        params = [
            bigquery.ArrayQueryParameter("team_ids", "INT64", team_ids),
            bigquery.ScalarQueryParameter("run_date", "DATE", TODAY.isoformat()),
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
        ]
        rows = bq.query(
            sql, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result()
    except Exception as exc:
        log.warning("Fallback recent-team batter rank lookup failed: %s", exc)
        return {}

    by_team = {}
    for row in rows:
        team_id   = si(row["batter_team_id"])
        batter_id = si(row["batter_id"])
        if team_id is None or batter_id is None:
            continue
        by_team.setdefault(team_id, []).append(batter_id)

    total = sum(len(v) for v in by_team.values())
    log.info(
        "Loaded ranking for %s fallback batters across %s teams",
        total,
        len(by_team),
    )
    return by_team


def rank_roster_players(team_id, roster_players, ranked_history):
    roster_ordered = unique_ints(roster_players)
    if not roster_ordered:
        return []

    history_rank  = {
        batter_id: idx
        for idx, batter_id in enumerate(ranked_history.get(team_id, []))
    }
    roster_index = {batter_id: idx for idx, batter_id in enumerate(roster_ordered)}
    return sorted(
        roster_ordered,
        key=lambda batter_id: (
            history_rank.get(batter_id, 10**9),
            roster_index[batter_id],
        ),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info("Starting PropFinder ingest for %s", TODAY)
    ensure_tables()

    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:

        # ── Step 1: fetch today's games + weather + odds ──────────────────────
        games = await fetch_upcoming_games(session)
        if not games:
            log.warning("No games found - exiting")
            return

        # ── Step 2: fetch expert weather notes ────────────────────────────────
        weather_notes = await fetch_weather_notes(session)

        # ── Step 2a: fetch all-team strikeout K rankings ──────────────────────
        team_ranking_rows = await fetch_teams(session)
        bq_insert("raw_team_strikeout_rankings", team_ranking_rows)

        # ── Step 2b: look up real pitcher names from MLB Stats API ────────────
        all_pitcher_ids = unique_ints(
            [game["home_pitcher_id"] for game in games if game.get("home_pitcher_id")]
            + [game["away_pitcher_id"] for game in games if game.get("away_pitcher_id")]
        )
        pitcher_name_map = await fetch_player_names(session, all_pitcher_ids)
        log.info("Fetched names for %s pitchers", len(pitcher_name_map))

        # ── Step 2c: fetch HR prop odds + K prop odds for each game ────────────
        props_results = await asyncio.gather(
            *[fetch_props(session, game["game_pk"]) for game in games]
        )
        props_by_game = {
            game["game_pk"]: result
            for game, result in zip(games, props_results)
        }

        k_props_results = await asyncio.gather(
            *[fetch_k_props(session, game["game_pk"]) for game in games]
        )
        all_k_props_rows = []
        for result in k_props_results:
            all_k_props_rows.extend(result)
        bq_insert("raw_k_props", all_k_props_rows)

        hit_props_results = await asyncio.gather(
            *[fetch_hit_props(session, game["game_pk"]) for game in games]
        )
        all_hit_props_rows = []
        for result in hit_props_results:
            all_hit_props_rows.extend(result)
        bq_insert("raw_hit_props", all_hit_props_rows)

        # ── Step 3: build raw_game_weather rows ───────────────────────────────
        game_weather_rows = []
        for game in games:
            note = weather_notes.get(game["game_pk"], {})
            game_weather_rows.append({
                "run_date":          TODAY.isoformat(),
                "game_pk":           game["game_pk"],
                "game_date":         game["game_date"],
                "home_team_id":      game["home_team_id"],
                "home_team_name":    game["home_team_name"],
                "away_team_id":      game["away_team_id"],
                "away_team_name":    game["away_team_name"],
                "weather_indicator": game["weather_indicator"],
                "game_temp":         game["game_temp"],
                "wind_speed":        game["wind_speed"],
                "wind_dir":          game["wind_dir"],
                "wind_gust":         game["wind_gust"],
                "precip_prob":       game["precip_prob"],
                "conditions":        game["conditions"],
                "ballpark_name":     game["ballpark_name"],
                "roof_type":         game["roof_type"],
                "ballpark_azimuth":  game.get("ballpark_azimuth"),
                "home_moneyline":    game["home_moneyline"],
                "away_moneyline":    game["away_moneyline"],
                "over_under":        game["over_under"],
                "weather_note":      note.get("content", ""),
                "ingested_at":       NOW.isoformat(),
            })

        bq_insert("raw_game_weather", game_weather_rows)

        # ── Step 4: build batter + pitcher jobs from batting orders ───────────
        team_ids = unique_ints(
            [game["home_team_id"] for game in games]
            + [game["away_team_id"] for game in games]
        )
        roster_results = await asyncio.gather(
            *[fetch_team_roster(session, team_id) for team_id in team_ids]
        )
        roster_by_team        = {
            team_id: players
            for team_id, players in zip(team_ids, roster_results)
        }
        ranked_history_by_team = load_recent_team_batter_rank(team_ids)

        batter_jobs  = []
        batter_seen  = set()
        pitcher_jobs = []

        for game in games:
            game_pk      = game["game_pk"]
            home_players = list(unique_ints(game["home_batting_order"]))
            away_players = list(unique_ints(game["away_batting_order"]))
            home_source  = "lineup" if home_players else "fallback"
            away_source  = "lineup" if away_players else "fallback"

            if not home_players:
                home_players = rank_roster_players(
                    game["home_team_id"],
                    roster_by_team.get(game["home_team_id"], []),
                    ranked_history_by_team,
                )
                if home_players:
                    home_source = "roster-ranked"
                else:
                    home_players = ranked_history_by_team.get(game["home_team_id"], [])
                    if home_players:
                        home_source = "recent-history"

            if not away_players:
                away_players = rank_roster_players(
                    game["away_team_id"],
                    roster_by_team.get(game["away_team_id"], []),
                    ranked_history_by_team,
                )
                if away_players:
                    away_source = "roster-ranked"
                else:
                    away_players = ranked_history_by_team.get(game["away_team_id"], [])
                    if away_players:
                        away_source = "recent-history"

            if home_source != "lineup" or away_source != "lineup":
                log.info(
                    "Lineup fallback game=%s home=%s(%s) away=%s(%s)",
                    game_pk,
                    len(home_players),
                    home_source,
                    len(away_players),
                    away_source,
                )

            for batter_id in home_players:
                batter_id_int = si(batter_id)
                if batter_id_int is None:
                    continue
                key = (batter_id_int, game_pk, game["home_team_id"])
                if key in batter_seen:
                    continue
                batter_seen.add(key)
                batter_jobs.append({
                    "batter_id":      batter_id_int,
                    "game_pk":        game_pk,
                    "batter_team_id": game["home_team_id"],
                })

            for batter_id in away_players:
                batter_id_int = si(batter_id)
                if batter_id_int is None:
                    continue
                key = (batter_id_int, game_pk, game["away_team_id"])
                if key in batter_seen:
                    continue
                batter_seen.add(key)
                batter_jobs.append({
                    "batter_id":      batter_id_int,
                    "game_pk":        game_pk,
                    "batter_team_id": game["away_team_id"],
                })

            if game["home_pitcher_id"]:
                pitcher_jobs.append({
                    "pitcher_id":   game["home_pitcher_id"],
                    "pitcher_name": pitcher_name_map.get(
                        game["home_pitcher_id"],
                        f"Pitcher {game['home_pitcher_id']}",
                    ),
                    "opp_team_id":  game["away_team_id"],
                    "game_pk":      game_pk,
                })
            if game["away_pitcher_id"]:
                pitcher_jobs.append({
                    "pitcher_id":   game["away_pitcher_id"],
                    "pitcher_name": pitcher_name_map.get(
                        game["away_pitcher_id"],
                        f"Pitcher {game['away_pitcher_id']}",
                    ),
                    "opp_team_id":  game["home_team_id"],
                    "game_pk":      game_pk,
                })

        log.info("Batter jobs: %s | Pitcher jobs: %s", len(batter_jobs), len(pitcher_jobs))

        # ── Step 5: fetch batter hit-data + splits ────────────────────────────
        all_hit_rows   = []
        all_split_rows = []

        async def fetch_batter(job):
            hit_rows    = await fetch_hit_data(
                session, job["batter_id"], job["game_pk"], job["batter_team_id"]
            )
            batter_name = hit_rows[0]["batter_name"] if hit_rows else str(job["batter_id"])
            split_rows  = await fetch_splits(session, job["batter_id"], batter_name)
            return hit_rows, split_rows

        batter_results = await asyncio.gather(*[fetch_batter(job) for job in batter_jobs])
        for hit_rows, split_rows in batter_results:
            all_hit_rows.extend(hit_rows)
            all_split_rows.extend(split_rows)

        # ── Step 6: fetch pitcher matchup data ────────────────────────────────
        all_pitcher_rows   = []
        all_pitch_log_rows = []

        pitcher_results = await asyncio.gather(
            *[
                fetch_pitcher_matchup(
                    session,
                    job["pitcher_id"],
                    job["opp_team_id"],
                    job["game_pk"],
                    job["pitcher_name"],
                )
                for job in pitcher_jobs
            ]
        )
        for result in pitcher_results:
            all_pitcher_rows.extend(result["splits"])
            all_pitch_log_rows.extend(result["pitch_log"])

        # ── Step 6b: fetch pitcher vs batting order splits ──────────────────
        batting_order_results = await asyncio.gather(
            *[
                fetch_batting_order_splits(
                    session,
                    job["pitcher_id"],
                    job["pitcher_name"],
                    "",  # hand filled by API response
                    job["opp_team_id"],
                    job["game_pk"],
                )
                for job in pitcher_jobs
            ]
        )
        all_batting_order_rows = []
        for result in batting_order_results:
            all_batting_order_rows.extend(result)

        # ── Step 6c: fetch PropFinder batter pitch-type stats ─────────────────
        unique_batter_ids = sorted({job["batter_id"] for job in batter_jobs})
        log.info("Fetching PropFinder pitchlog for %s unique batters", len(unique_batter_ids))
        all_statcast_rows = await fetch_batter_pitch_stats(session, unique_batter_ids)

        bq_insert("raw_hit_data",          all_hit_rows)
        bq_insert("raw_splits",            all_split_rows)
        bq_insert("raw_pitcher_matchup",   all_pitcher_rows)
        bq_insert("raw_pitch_log",         all_pitch_log_rows)
        bq_insert("raw_pitcher_vs_batting_order", all_batting_order_rows)
        bq_insert("raw_statcast_batter_pitch_stats", all_statcast_rows)

        # ── Step 7: insert HR prop odds ───────────────────────────────────────
        all_props_rows = []
        for game_pk_key, player_props in props_by_game.items():
            for player_id, p in player_props.items():
                all_props_rows.append({
                    "run_date":           TODAY.isoformat(),
                    "game_pk":            game_pk_key,
                    "player_id":          player_id,
                    "player_name":        p["player_name"],
                    "hr_odds_best_price": p["hr_odds_best_price"],
                    "hr_odds_best_book":  p["hr_odds_best_book"],
                    "deep_link_desktop":  p["deep_link_desktop"],
                    "deep_link_ios":      p["deep_link_ios"],
                    "dk_outcome_code":    p["dk_outcome_code"],
                    "dk_event_id":        p["dk_event_id"],
                    "fd_market_id":       p["fd_market_id"],
                    "fd_selection_id":    p["fd_selection_id"],
                    "ingested_at":        NOW.isoformat(),
                })
        bq_insert("raw_hr_props", all_props_rows)

        log.info("Ingest complete.")


if __name__ == "__main__":
    asyncio.run(main())