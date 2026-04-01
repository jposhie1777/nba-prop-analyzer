#mobile_api/routes/mlb_matchups.py
from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(tags=["MLB Matchups"])

NY_TZ = ZoneInfo("America/New_York")
MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
PROPFINDER_MLB_PROPS_URL = os.getenv("PROPFINDER_MLB_PROPS_URL", "https://api.propfinder.app/mlb/props")

PROPFINDER_DATASET = os.getenv("PROPFINDER_DATASET", "propfinder")
K_SIGNAL_VIEW = os.getenv("PROPFINDER_K_SIGNAL_VIEW", f"{PROPFINDER_DATASET}.vw_strikeout_signal")
K_GRADES_VIEW = os.getenv("PROPFINDER_K_GRADES_VIEW", f"{PROPFINDER_DATASET}.vw_k_prop_grades")
K_PROPS_TABLE = os.getenv("PROPFINDER_K_PROPS_TABLE", f"{PROPFINDER_DATASET}.raw_k_props")
TEAM_K_RANKINGS_TABLE = os.getenv("PROPFINDER_TEAM_K_TABLE", f"{PROPFINDER_DATASET}.raw_team_strikeout_rankings")
BATTING_ORDER_TABLE = os.getenv("PROPFINDER_BATTING_ORDER_TABLE", f"{PROPFINDER_DATASET}.raw_pitcher_vs_batting_order")
HR_PICKS_TABLE = os.getenv("PROPFINDER_HR_PICKS_TABLE", f"{PROPFINDER_DATASET}.hr_picks_daily")
PITCHER_MATCHUP_TABLE = os.getenv(
    "PROPFINDER_PITCHER_MATCHUP_TABLE",
    f"{PROPFINDER_DATASET}.raw_pitcher_matchup",
)
PITCH_LOG_TABLE = os.getenv(
    "PROPFINDER_PITCH_LOG_TABLE",
    f"{PROPFINDER_DATASET}.raw_pitch_log",
)
HIT_DATA_TABLE = os.getenv(
    "PROPFINDER_HIT_DATA_TABLE",
    f"{PROPFINDER_DATASET}.raw_hit_data",
)
GAME_WEATHER_TABLE = os.getenv(
    "PROPFINDER_GAME_WEATHER_TABLE",
    f"{PROPFINDER_DATASET}.raw_game_weather",
)


def _today_et_iso() -> str:
    return datetime.now(NY_TZ).date().isoformat()


def _qualified_table(client: bigquery.Client, table_ref: str) -> str:
    parts = table_ref.split(".")
    if len(parts) == 2:
        return f"`{client.project}.{parts[0]}.{parts[1]}`"
    if len(parts) == 3:
        return f"`{table_ref}`"
    raise ValueError(f"Unsupported BigQuery table reference: {table_ref}")


def _query(
    client: bigquery.Client,
    sql: str,
    params: List[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter],
) -> List[Dict[str, Any]]:
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    )
    return [dict(row) for row in job.result()]


def _safe_query(
    client: bigquery.Client,
    sql: str,
    params: List[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter],
) -> List[Dict[str, Any]]:
    try:
        return _query(client, sql, params)
    except Exception:
        return []


def _exception_message(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _fetch_schedule_raw(params: Dict[str, Any]) -> Dict[str, Any]:
    query = urlencode(params)
    url = f"{MLB_SCHEDULE_URL}?{query}"
    request = Request(
        url,
        headers={
            "User-Agent": "PulseSports/1.0",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def _parse_schedule_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for date_block in payload.get("dates", []):
        for game in date_block.get("games", []):
            teams = game.get("teams", {})
            home = teams.get("home", {}) or {}
            away = teams.get("away", {}) or {}
            home_team = (home.get("team") or {}).get("name")
            away_team = (away.get("team") or {}).get("name")
            out.append(
                {
                    "game_pk": game.get("gamePk"),
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_team_id": (home.get("team") or {}).get("id"),
                    "away_team_id": (away.get("team") or {}).get("id"),
                    "start_time_utc": game.get("gameDate"),
                    "venue_name": (game.get("venue") or {}).get("name"),
                    "home_pitcher_name": (home.get("probablePitcher") or {}).get("fullName"),
                    "away_pitcher_name": (away.get("probablePitcher") or {}).get("fullName"),
                }
            )
    out.sort(key=lambda row: row.get("start_time_utc") or "")
    return out


def _fetch_schedule_for_today() -> List[Dict[str, Any]]:
    today = datetime.now(NY_TZ).date()
    target_dates = [today, today + timedelta(days=1)]
    combined: List[Dict[str, Any]] = []
    seen_game_pks: set[int] = set()

    for target_date in target_dates:
        date_iso = target_date.isoformat()
        try:
            payload = _fetch_schedule_raw(
                {
                    "sportId": 1,
                    "date": date_iso,
                    "hydrate": "probablePitcher,team,venue",
                }
            )
        except Exception:
            continue

        rows = _parse_schedule_rows(payload)
        for row in rows:
            game_pk = _safe_int(row.get("game_pk"))
            if game_pk is not None and game_pk in seen_game_pks:
                continue
            if game_pk is not None:
                seen_game_pks.add(game_pk)
            combined.append(row)

        # If we found games for today, no need to query farther ahead.
        if rows and target_date == today:
            break

    combined.sort(key=lambda row: row.get("start_time_utc") or "")
    return combined


def _fetch_schedule_for_date_iso(date_iso: str) -> List[Dict[str, Any]]:
    try:
        payload = _fetch_schedule_raw(
            {
                "sportId": 1,
                "date": date_iso,
                "hydrate": "probablePitcher,team,venue",
            }
        )
    except Exception:
        return []
    return _parse_schedule_rows(payload)


def _fetch_schedule_for_date_iso_debug(date_iso: str) -> Dict[str, Any]:
    try:
        payload = _fetch_schedule_raw(
            {
                "sportId": 1,
                "date": date_iso,
                "hydrate": "probablePitcher,team,venue",
            }
        )
        rows = _parse_schedule_rows(payload)
        return {
            "date": date_iso,
            "ok": True,
            "rows_count": len(rows),
            "total_games": (payload.get("dates") or [{}])[0].get("totalGames", 0),
            "sample_game_pks": [row.get("game_pk") for row in rows[:5]],
        }
    except Exception as exc:
        return {
            "date": date_iso,
            "ok": False,
            "error": _exception_message(exc),
            "rows_count": 0,
            "total_games": 0,
            "sample_game_pks": [],
        }


def _fetch_schedule_for_game(game_pk: int) -> Optional[Dict[str, Any]]:
    try:
        payload = _fetch_schedule_raw(
            {
                "sportId": 1,
                "gamePk": game_pk,
                "hydrate": "probablePitcher,team,venue",
            }
        )
    except Exception:
        return None
    rows = _parse_schedule_rows(payload)
    return rows[0] if rows else None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_book(book_name: Any) -> str:
    book = str(book_name or "").strip().lower().replace(" ", "")
    if not book:
        return ""
    if book in ("dk", "draftkings"):
        return "draftkings"
    if book in ("fd", "fanduel"):
        return "fanduel"
    return book


def _parse_draftkings_link(url: Any) -> tuple[Optional[str], Optional[str]]:
    text = _clean_str(url)
    if not text:
        return None, None
    split = urlsplit(text)
    if "draftkings.com" not in split.netloc.lower():
        return None, None
    path_parts = [part for part in split.path.split("/") if part]
    event_id = None
    for idx, part in enumerate(path_parts):
        if part == "event" and idx + 1 < len(path_parts):
            event_id = path_parts[idx + 1]
            break
    outcome_code = None
    for part in (split.query or "").split("&"):
        if part.startswith("outcomes="):
            # Keep the raw encoded value so downstream deep links are valid.
            outcome_code = part.split("=", 1)[1].strip() or None
            break
    return _clean_str(event_id), _clean_str(outcome_code)


def _parse_fanduel_link(url: Any) -> tuple[Optional[str], Optional[str]]:
    text = _clean_str(url)
    if not text:
        return None, None
    split = urlsplit(text)
    if "fanduel.com" not in split.netloc.lower():
        return None, None
    query = parse_qs(split.query, keep_blank_values=False)
    market_id = (query.get("marketId") or query.get("marketId[]") or [None])[0]
    selection_id = (query.get("selectionId") or query.get("selectionId[]") or [None])[0]
    return _clean_str(market_id), _clean_str(selection_id)


def _load_live_hr_props_context(game_pk: int) -> Dict[int, Dict[str, Any]]:
    """
    Fallback context from live /mlb/props for batters in a game.
    Keyed by batter_id. Only tracks 1+ HR over rows.
    """
    url = f"{PROPFINDER_MLB_PROPS_URL}?date={_today_et_iso()}"
    request = Request(
        url,
        headers={
            "User-Agent": "PulseSports/1.0",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, list):
        return {}

    by_batter: Dict[int, Dict[str, Any]] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        if _safe_int(row.get("gameId")) != game_pk:
            continue
        category = str(row.get("category") or "").lower().replace("_", "")
        if "homerun" not in category:
            continue
        if str(row.get("overUnder") or "").lower() != "over":
            continue
        line = _safe_float(row.get("line"))
        if line is None or abs(line - 0.5) > 1e-6:
            continue
        batter_id = _safe_int(row.get("playerId"))
        if batter_id is None:
            continue

        best_price: Optional[int] = None
        best_book: Optional[str] = None
        best_desktop: Optional[str] = None
        best_ios: Optional[str] = None
        dk_event_id: Optional[str] = None
        dk_outcome_code: Optional[str] = None
        fd_market_id: Optional[str] = None
        fd_selection_id: Optional[str] = None

        markets = row.get("markets") if isinstance(row.get("markets"), list) else []
        best_market = row.get("bestMarket") if isinstance(row.get("bestMarket"), dict) else None
        all_markets = [m for m in markets if isinstance(m, dict)]
        if best_market:
            all_markets.append(best_market)

        for market in all_markets:
            sportsbook = _clean_str(market.get("sportsbook"))
            desktop_link = _clean_str(market.get("deepLinkDesktop"))
            ios_link = (
                _clean_str(market.get("deepLinkIos"))
                or _clean_str(market.get("deepLinkIOS"))
                or _clean_str(market.get("deepLinkAndroid"))
            )
            price = _safe_int(market.get("price"))
            if price is not None and (best_price is None or price > best_price):
                best_price = price
                best_book = sportsbook
                best_desktop = desktop_link
                best_ios = ios_link

            book_key = _normalize_book(sportsbook)
            if book_key == "draftkings" and desktop_link:
                event_id, outcome_code = _parse_draftkings_link(desktop_link)
                dk_event_id = dk_event_id or event_id
                dk_outcome_code = dk_outcome_code or outcome_code
            if book_key == "fanduel" and desktop_link:
                market_id, selection_id = _parse_fanduel_link(desktop_link)
                fd_market_id = fd_market_id or market_id
                fd_selection_id = fd_selection_id or selection_id

        by_batter[batter_id] = {
            "hr_odds_best_price": best_price,
            "hr_odds_best_book": best_book,
            "deep_link_desktop": best_desktop,
            "deep_link_ios": best_ios,
            "dk_event_id": dk_event_id,
            "dk_outcome_code": dk_outcome_code,
            "fd_market_id": fd_market_id,
            "fd_selection_id": fd_selection_id,
        }
    return by_batter


def _merge_props_fallback_into_picks(picks: List[Dict[str, Any]], game_pk: int) -> List[Dict[str, Any]]:
    """
    Fill missing sportsbook fields from live /mlb/props context.
    """
    if not picks:
        return picks
    live_ctx = _load_live_hr_props_context(game_pk)
    if not live_ctx:
        return picks

    out: List[Dict[str, Any]] = []
    for pick in picks:
        row = dict(pick)
        batter_id = _safe_int(row.get("batter_id"))
        if batter_id is None:
            out.append(row)
            continue
        fallback = live_ctx.get(batter_id)
        if not fallback:
            out.append(row)
            continue

        for key in (
            "hr_odds_best_price",
            "hr_odds_best_book",
            "deep_link_desktop",
            "deep_link_ios",
            "dk_event_id",
            "dk_outcome_code",
            "fd_market_id",
            "fd_selection_id",
        ):
            current = row.get(key)
            if current is None or (isinstance(current, str) and not current.strip()):
                row[key] = fallback.get(key)
        out.append(row)
    return out


def _fetch_hr_prop_link_map_for_game(game_pk: int, date_iso: str) -> tuple[Dict[int, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Fetch live HR 1+ prop links as a fallback when hr_picks_daily rows
    are missing sportsbook link metadata.
    """
    request = Request(
        f"{PROPFINDER_MLB_PROPS_URL}?date={date_iso}",
        headers={
            "User-Agent": "PulseSports/1.0",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return {}, {}

    if not isinstance(payload, list):
        return {}, {}

    by_id: Dict[int, Dict[str, Any]] = {}
    by_name: Dict[str, Dict[str, Any]] = {}

    for row in payload:
        if not isinstance(row, dict):
            continue
        if _safe_int(row.get("gameId")) != game_pk:
            continue
        category = str(row.get("category") or "").lower().replace("_", "")
        if "homerun" not in category:
            continue
        if str(row.get("overUnder") or "").lower() != "over":
            continue
        line = _safe_float(row.get("line"))
        if line is not None and abs(line - 0.5) > 1e-6:
            continue

        batter_id = _safe_int(row.get("playerId"))
        batter_name = _clean_str(row.get("name"))
        if batter_id is None and not batter_name:
            continue

        markets = row.get("markets") if isinstance(row.get("markets"), list) else []
        best_market = row.get("bestMarket") if isinstance(row.get("bestMarket"), dict) else None
        market_rows = [m for m in markets if isinstance(m, dict)]
        if best_market:
            market_rows.append(best_market)

        best_price = None
        best_book = None
        best_desktop = None
        best_ios = None
        dk_event_id = None
        dk_outcome_code = None
        fd_market_id = None
        fd_selection_id = None

        for market in market_rows:
            sportsbook = _clean_str(market.get("sportsbook"))
            desktop_link = _clean_str(market.get("deepLinkDesktop"))
            ios_link = (
                _clean_str(market.get("deepLinkIos"))
                or _clean_str(market.get("deepLinkIOS"))
                or _clean_str(market.get("deepLinkAndroid"))
            )
            price = _safe_int(market.get("price"))

            if price is not None and (best_price is None or price > best_price):
                best_price = price
                best_book = sportsbook
                best_desktop = desktop_link
                best_ios = ios_link

            book_key = _normalize_book(sportsbook)
            if book_key == "draftkings" and desktop_link:
                event_id, outcome_code = _parse_draftkings_link(desktop_link)
                dk_event_id = dk_event_id or event_id
                dk_outcome_code = dk_outcome_code or outcome_code
            if book_key == "fanduel" and desktop_link:
                market_id, selection_id = _parse_fanduel_link(desktop_link)
                fd_market_id = fd_market_id or market_id
                fd_selection_id = fd_selection_id or selection_id

        parsed = {
            "hr_odds_best_price": best_price,
            "hr_odds_best_book": best_book,
            "deep_link_desktop": best_desktop,
            "deep_link_ios": best_ios,
            "dk_outcome_code": dk_outcome_code,
            "dk_event_id": dk_event_id,
            "fd_market_id": fd_market_id,
            "fd_selection_id": fd_selection_id,
        }
        if batter_id is not None:
            by_id[batter_id] = parsed
        if batter_name:
            by_name[batter_name.strip().lower()] = parsed

    return by_id, by_name


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _wind_direction_label(degrees: Optional[int]) -> Optional[str]:
    if degrees is None:
        return None
    labels = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    normalized = degrees % 360
    index = int((normalized + 11.25) // 22.5) % 16
    return labels[index]


def _parse_flags(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if not isinstance(value, str):
        return [str(value)]
    text = value.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return [str(parsed)]
    except Exception:
        return [text]


def _normalized_name(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _pitcher_group_key(pitcher_id: Optional[int], pitcher_name: Any) -> Optional[str]:
    if pitcher_id is not None:
        return f"id:{pitcher_id}"
    name = _normalized_name(pitcher_name)
    if name:
        return f"name:{name}"
    return None


def _normalize_hand(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if text.startswith("L"):
        return "L"
    if text.startswith("R"):
        return "R"
    return None


def _normalize_pitch_name(value: Any) -> Optional[str]:
    text = _clean_str(value)
    if not text:
        return None
    return text.strip().lower()


def _pct_value(value: Any) -> Optional[float]:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return parsed * 100.0 if 0 <= parsed <= 1 else parsed


def _fetch_pitcher_pitch_mix_map(
    client: bigquery.Client,
    pitch_log_table_qualified: str,
    hit_data_table_qualified: str,
    run_date: str,
    game_pk: int,
    pitcher_ids: List[int],
) -> Dict[int, Dict[str, List[Dict[str, Any]]]]:
    """
    Build pitcher pitch-mix rows keyed by pitcher_id and batter hand (L/R).
    """
    pitcher_ids = sorted({int(pid) for pid in pitcher_ids if pid is not None})
    if not pitcher_ids:
        return {}

    pitch_rows = _safe_query(
        client,
        f"""
        SELECT
          CAST(pitcher_id AS INT64) AS pitcher_id,
          UPPER(CAST(batter_hand AS STRING)) AS batter_hand,
          CAST(pitch_name AS STRING) AS pitch_name,
          CAST(count AS INT64) AS pitch_count,
          CAST(percentage AS FLOAT64) AS pitch_pct,
          CAST(woba AS FLOAT64) AS woba,
          CAST(slg AS FLOAT64) AS slg,
          CAST(iso AS FLOAT64) AS iso,
          CAST(home_runs AS INT64) AS hr,
          CAST(k_percent AS FLOAT64) AS k_pct,
          CAST(whiff AS FLOAT64) AS whiff_pct
        FROM {pitch_log_table_qualified}
        WHERE run_date = @run_date
          AND CAST(game_pk AS INT64) = @game_pk
          AND CAST(pitcher_id AS INT64) IN UNNEST(@pitcher_ids)
          AND COALESCE(CAST(pitch_name AS STRING), '') != ''
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY pitcher_id, batter_hand, pitch_name
          ORDER BY ingested_at DESC NULLS LAST
        ) = 1
        """,
        [
            bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
            bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
            bigquery.ArrayQueryParameter("pitcher_ids", "INT64", pitcher_ids),
        ],
    )

    pitcher_ba_rows = _safe_query(
        client,
        f"""
        SELECT
          CAST(pitcher_id AS INT64) AS pitcher_id,
          UPPER(CAST(bat_side AS STRING)) AS batter_hand,
          CAST(pitch_type AS STRING) AS pitch_name,
          AVG(
            CASE
              WHEN LOWER(CAST(result AS STRING)) IN ('single', 'double', 'triple', 'home_run') THEN 1.0
              ELSE 0.0
            END
          ) AS ba
        FROM {hit_data_table_qualified}
        WHERE run_date = @run_date
          AND CAST(game_pk AS INT64) = @game_pk
          AND CAST(pitcher_id AS INT64) IN UNNEST(@pitcher_ids)
          AND UPPER(CAST(bat_side AS STRING)) IN ('L', 'R')
          AND COALESCE(CAST(pitch_type AS STRING), '') != ''
        GROUP BY pitcher_id, batter_hand, pitch_name
        """,
        [
            bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
            bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
            bigquery.ArrayQueryParameter("pitcher_ids", "INT64", pitcher_ids),
        ],
    )

    ba_map: Dict[tuple[int, str, str], Optional[float]] = {}
    for row in pitcher_ba_rows:
        pitcher_id = _safe_int(row.get("pitcher_id"))
        batter_hand = _normalize_hand(row.get("batter_hand"))
        pitch_name_norm = _normalize_pitch_name(row.get("pitch_name"))
        if pitcher_id is None or batter_hand is None or pitch_name_norm is None:
            continue
        ba_map[(pitcher_id, batter_hand, pitch_name_norm)] = _safe_float(row.get("ba"))

    out: Dict[int, Dict[str, List[Dict[str, Any]]]] = {pitcher_id: {"L": [], "R": []} for pitcher_id in pitcher_ids}
    for row in pitch_rows:
        pitcher_id = _safe_int(row.get("pitcher_id"))
        batter_hand = _normalize_hand(row.get("batter_hand"))
        pitch_name = _clean_str(row.get("pitch_name"))
        if pitcher_id is None or batter_hand is None or pitch_name is None:
            continue
        pitch_name_norm = _normalize_pitch_name(pitch_name)
        if pitch_name_norm is None:
            continue
        out.setdefault(pitcher_id, {"L": [], "R": []}).setdefault(batter_hand, [])
        out[pitcher_id][batter_hand].append(
            {
                "pitch_name": pitch_name,
                "pitch_count": _safe_int(row.get("pitch_count")) or 0,
                "pitch_pct": _pct_value(row.get("pitch_pct")),
                "ba": ba_map.get((pitcher_id, batter_hand, pitch_name_norm)),
                "woba": _safe_float(row.get("woba")),
                "slg": _safe_float(row.get("slg")),
                "iso": _safe_float(row.get("iso")),
                "hr": _safe_int(row.get("hr")) or 0,
                "k_pct": _pct_value(row.get("k_pct")),
                "whiff_pct": _pct_value(row.get("whiff_pct")),
            }
        )

    for pitcher_id, hand_map in out.items():
        for hand in ("L", "R"):
            rows = hand_map.get(hand, [])
            total_count = sum((_safe_int(item.get("pitch_count")) or 0) for item in rows)
            for item in rows:
                if item.get("pitch_pct") is None and total_count > 0:
                    item["pitch_pct"] = round(((_safe_int(item.get("pitch_count")) or 0) / total_count) * 100.0, 1)
            rows.sort(key=lambda item: ((item.get("pitch_pct") or 0.0), (_safe_int(item.get("pitch_count")) or 0)), reverse=True)
            hand_map[hand] = rows
        out[pitcher_id] = hand_map

    return out


def _fetch_batter_vs_pitches_map(
    client: bigquery.Client,
    hit_data_table_qualified: str,
    run_date: str,
    game_pk: int,
    batter_ids: List[int],
) -> Dict[int, Dict[str, List[Dict[str, Any]]]]:
    """
    Build hitter-vs-pitch rows keyed by batter_id and pitcher hand (L/R).
    """
    batter_ids = sorted({int(bid) for bid in batter_ids if bid is not None})
    if not batter_ids:
        return {}

    rows = _safe_query(
        client,
        f"""
        SELECT
          CAST(batter_id AS INT64) AS batter_id,
          UPPER(CAST(pitch_hand AS STRING)) AS pitcher_hand,
          CAST(pitch_type AS STRING) AS pitch_name,
          COUNT(1) AS pitch_count,
          AVG(
            CASE
              WHEN LOWER(CAST(result AS STRING)) IN ('single', 'double', 'triple', 'home_run') THEN 1.0
              ELSE 0.0
            END
          ) AS ba,
          AVG(
            CASE LOWER(CAST(result AS STRING))
              WHEN 'single' THEN 0.89
              WHEN 'double' THEN 1.27
              WHEN 'triple' THEN 1.62
              WHEN 'home_run' THEN 2.10
              ELSE 0.0
            END
          ) AS woba,
          AVG(
            CASE LOWER(CAST(result AS STRING))
              WHEN 'single' THEN 1.0
              WHEN 'double' THEN 2.0
              WHEN 'triple' THEN 3.0
              WHEN 'home_run' THEN 4.0
              ELSE 0.0
            END
          ) AS slg,
          AVG(
            CASE LOWER(CAST(result AS STRING))
              WHEN 'double' THEN 1.0
              WHEN 'triple' THEN 2.0
              WHEN 'home_run' THEN 3.0
              ELSE 0.0
            END
          ) AS iso,
          SUM(CASE WHEN LOWER(CAST(result AS STRING)) = 'home_run' THEN 1 ELSE 0 END) AS hr,
          AVG(CASE WHEN is_barrel THEN 1.0 ELSE 0.0 END) * 100.0 AS barrel_pct,
          AVG(CAST(launch_speed AS FLOAT64)) AS ev
        FROM {hit_data_table_qualified}
        WHERE run_date = @run_date
          AND CAST(game_pk AS INT64) = @game_pk
          AND CAST(batter_id AS INT64) IN UNNEST(@batter_ids)
          AND UPPER(CAST(pitch_hand AS STRING)) IN ('L', 'R', 'LHP', 'RHP')
          AND COALESCE(CAST(pitch_type AS STRING), '') != ''
        GROUP BY batter_id, pitcher_hand, pitch_name
        """,
        [
            bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
            bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
            bigquery.ArrayQueryParameter("batter_ids", "INT64", batter_ids),
        ],
    )

    out: Dict[int, Dict[str, List[Dict[str, Any]]]] = {batter_id: {"L": [], "R": []} for batter_id in batter_ids}
    totals: Dict[tuple[int, str], int] = {}

    for row in rows:
        batter_id = _safe_int(row.get("batter_id"))
        pitcher_hand = _normalize_hand(row.get("pitcher_hand"))
        pitch_name = _clean_str(row.get("pitch_name"))
        if batter_id is None or pitcher_hand is None or pitch_name is None:
            continue
        count = _safe_int(row.get("pitch_count")) or 0
        totals[(batter_id, pitcher_hand)] = totals.get((batter_id, pitcher_hand), 0) + count
        out.setdefault(batter_id, {"L": [], "R": []}).setdefault(pitcher_hand, [])
        out[batter_id][pitcher_hand].append(
            {
                "pitch_name": pitch_name,
                "pitch_count": count,
                "pitch_pct": None,
                "ba": _safe_float(row.get("ba")),
                "woba": _safe_float(row.get("woba")),
                "slg": _safe_float(row.get("slg")),
                "iso": _safe_float(row.get("iso")),
                "hr": _safe_int(row.get("hr")) or 0,
                "ev": _safe_float(row.get("ev")),
                "barrel_pct": _safe_float(row.get("barrel_pct")),
            }
        )

    for batter_id, hand_map in out.items():
        for hand in ("L", "R"):
            total = totals.get((batter_id, hand), 0)
            rows_for_hand = hand_map.get(hand, [])
            for row in rows_for_hand:
                if total > 0:
                    row["pitch_pct"] = round(((_safe_int(row.get("pitch_count")) or 0) / total) * 100.0, 1)
            rows_for_hand.sort(key=lambda item: (_safe_int(item.get("pitch_count")) or 0), reverse=True)
            hand_map[hand] = rows_for_hand
        out[batter_id] = hand_map

    return out


def _fetch_bvp_career_map(
    client: bigquery.Client,
    hit_data_table_qualified: str,
    run_date: str,
    game_pk: int,
    batter_ids: List[int],
    pitcher_ids: List[int],
) -> Dict[str, Dict[str, Any]]:
    """
    Aggregate career batter-vs-specific-pitcher stats from raw_hit_data.
    Returns dict keyed by 'batter_id:pitcher_id'.
    """
    batter_ids = sorted({int(bid) for bid in batter_ids if bid is not None})
    pitcher_ids = sorted({int(pid) for pid in pitcher_ids if pid is not None})
    if not batter_ids or not pitcher_ids:
        return {}

    rows = _safe_query(
        client,
        f"""
        SELECT
          CAST(batter_id AS INT64) AS batter_id,
          CAST(pitcher_id AS INT64) AS pitcher_id,
          COUNT(1) AS pa,
          SUM(CASE WHEN LOWER(CAST(result AS STRING)) IN ('single','double','triple','home_run') THEN 1 ELSE 0 END) AS hits,
          SUM(CASE WHEN LOWER(CAST(result AS STRING)) = 'home_run' THEN 1 ELSE 0 END) AS hr,
          AVG(
            CASE WHEN LOWER(CAST(result AS STRING)) IN ('single','double','triple','home_run') THEN 1.0 ELSE 0.0 END
          ) AS avg,
          AVG(
            CASE LOWER(CAST(result AS STRING))
              WHEN 'double' THEN 1.0
              WHEN 'triple' THEN 2.0
              WHEN 'home_run' THEN 3.0
              ELSE 0.0
            END
          ) AS iso,
          AVG(
            CASE LOWER(CAST(result AS STRING))
              WHEN 'single' THEN 1.0
              WHEN 'double' THEN 2.0
              WHEN 'triple' THEN 3.0
              WHEN 'home_run' THEN 4.0
              ELSE 0.0
            END
          ) AS slg,
          SAFE_DIVIDE(
            SUM(CASE WHEN LOWER(CAST(result AS STRING)) IN ('single','double','triple','home_run','walk','hit_by_pitch') THEN 1 ELSE 0 END),
            NULLIF(COUNT(1), 0)
          ) AS obp,
          SAFE_DIVIDE(
            SUM(CASE WHEN LOWER(CAST(result AS STRING)) IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) * 100.0,
            NULLIF(COUNT(1), 0)
          ) AS k_pct,
          SAFE_DIVIDE(
            SUM(CASE WHEN LOWER(CAST(result AS STRING)) = 'walk' THEN 1 ELSE 0 END) * 100.0,
            NULLIF(COUNT(1), 0)
          ) AS bb_pct
        FROM {hit_data_table_qualified}
        WHERE run_date = @run_date
          AND CAST(game_pk AS INT64) = @game_pk
          AND CAST(batter_id AS INT64) IN UNNEST(@batter_ids)
          AND CAST(pitcher_id AS INT64) IN UNNEST(@pitcher_ids)
        GROUP BY batter_id, pitcher_id
        """,
        [
            bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
            bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
            bigquery.ArrayQueryParameter("batter_ids", "INT64", batter_ids),
            bigquery.ArrayQueryParameter("pitcher_ids", "INT64", pitcher_ids),
        ],
    )

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        bid = _safe_int(row.get("batter_id"))
        pid = _safe_int(row.get("pitcher_id"))
        if bid is None or pid is None:
            continue
        out[f"{bid}:{pid}"] = {
            "pa": _safe_int(row.get("pa")) or 0,
            "hits": _safe_int(row.get("hits")) or 0,
            "hr": _safe_int(row.get("hr")) or 0,
            "avg": _safe_float(row.get("avg")),
            "iso": _safe_float(row.get("iso")),
            "slg": _safe_float(row.get("slg")),
            "obp": _safe_float(row.get("obp")),
            "k_pct": _safe_float(row.get("k_pct")),
            "bb_pct": _safe_float(row.get("bb_pct")),
        }
    return out


def _fetch_game_weather_map(
    client: bigquery.Client,
    weather_table_qualified: str,
    run_date: str,
    game_pks: Optional[List[int]] = None,
) -> Dict[int, Dict[str, Any]]:
    """Query raw_game_weather and return dict keyed by game_pk."""
    try:
        if game_pks:
            rows = _safe_query(
                client,
                f"""
                SELECT
                  CAST(game_pk AS INT64) AS game_pk,
                  weather_indicator,
                  game_temp,
                  wind_speed,
                  wind_dir,
                  wind_gust,
                  precip_prob,
                  conditions,
                  ballpark_name,
                  roof_type,
                  ballpark_azimuth,
                  home_moneyline,
                  away_moneyline,
                  over_under,
                  weather_note
                FROM {weather_table_qualified}
                WHERE run_date = @run_date
                  AND CAST(game_pk AS INT64) IN UNNEST(@game_pks)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY ingested_at DESC) = 1
                """,
                [
                    bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                    bigquery.ArrayQueryParameter("game_pks", "INT64", game_pks),
                ],
            )
        else:
            rows = _safe_query(
                client,
                f"""
                SELECT
                  CAST(game_pk AS INT64) AS game_pk,
                  weather_indicator,
                  game_temp,
                  wind_speed,
                  wind_dir,
                  wind_gust,
                  precip_prob,
                  conditions,
                  ballpark_name,
                  roof_type,
                  ballpark_azimuth,
                  home_moneyline,
                  away_moneyline,
                  over_under,
                  weather_note
                FROM {weather_table_qualified}
                WHERE run_date = @run_date
                QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY ingested_at DESC) = 1
                """,
                [bigquery.ScalarQueryParameter("run_date", "DATE", run_date)],
            )
        return {int(row["game_pk"]): row for row in rows if row.get("game_pk") is not None}
    except Exception:
        return {}


def _resolve_latest_run_date_for_game(
    client: bigquery.Client,
    table_ref_qualified: str,
    game_pk: int,
    preferred_date: str,
) -> str:
    row = _safe_query(
        client,
        f"""
        SELECT CAST(MAX(run_date) AS STRING) AS run_date
        FROM {table_ref_qualified}
        WHERE CAST(game_pk AS INT64) = @game_pk
        """,
        [bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk)],
    )
    latest = row[0].get("run_date") if row else None
    if isinstance(latest, str) and latest:
        return latest
    return _resolve_latest_run_date(client, table_ref_qualified, preferred_date)


def _resolve_latest_run_date(
    client: bigquery.Client,
    table_ref_qualified: str,
    preferred_date: str,
) -> str:
    row = _safe_query(
        client,
        f"""
        SELECT CAST(MAX(run_date) AS STRING) AS run_date
        FROM {table_ref_qualified}
        """,
        [],
    )
    latest = row[0].get("run_date") if row else None
    if isinstance(latest, str) and latest:
        return latest
    return preferred_date


@router.get("/mlb/matchups/upcoming")
def mlb_matchups_upcoming(
    limit: int = Query(default=20, ge=1, le=100),
):
    schedule_rows = _fetch_schedule_for_today()
    if not schedule_rows:
        return []

    client = get_bq_client()
    today = _today_et_iso()
    hr_table = _qualified_table(client, HR_PICKS_TABLE)
    weather_table = _qualified_table(client, GAME_WEATHER_TABLE)
    run_date = _resolve_latest_run_date(client, hr_table, today)

    summary_rows = _safe_query(
        client,
        f"""
        SELECT
          CAST(game_pk AS INT64) AS game_pk,
          COUNT(*) AS picks_count,
          MAX(score) AS top_score,
          ARRAY_AGG(grade ORDER BY score DESC LIMIT 1)[SAFE_OFFSET(0)] AS top_grade
        FROM {hr_table}
        WHERE run_date = @run_date
        GROUP BY game_pk
        """,
        [bigquery.ScalarQueryParameter("run_date", "DATE", run_date)],
    )
    summary_map = {int(row["game_pk"]): row for row in summary_rows if row.get("game_pk") is not None}

    schedule_game_pks = [_safe_int(g.get("game_pk")) for g in schedule_rows if g.get("game_pk") is not None]
    weather_map = _fetch_game_weather_map(client, weather_table, run_date, schedule_game_pks)

    rows: List[Dict[str, Any]] = []
    for game in schedule_rows[:limit]:
        game_pk = _safe_int(game.get("game_pk"))
        summary = summary_map.get(game_pk) if game_pk is not None else None
        gw = weather_map.get(game_pk) if game_pk is not None else None
        rows.append(
            {
                "game_pk": game_pk,
                "home_team": game.get("home_team"),
                "away_team": game.get("away_team"),
                "start_time_utc": game.get("start_time_utc"),
                "venue_name": gw.get("ballpark_name") if gw else game.get("venue_name"),
                "home_pitcher_name": game.get("home_pitcher_name"),
                "away_pitcher_name": game.get("away_pitcher_name"),
                "has_model_data": bool(summary),
                "picks_count": _safe_int(summary.get("picks_count")) if summary else 0,
                "top_score": _safe_float(summary.get("top_score")) if summary else None,
                "top_grade": summary.get("top_grade") if summary else None,
                # Weather fields
                "weather_indicator": gw.get("weather_indicator") if gw else None,
                "game_temp": _safe_float(gw.get("game_temp")) if gw else None,
                "wind_speed": _safe_float(gw.get("wind_speed")) if gw else None,
                "wind_dir": _safe_int(gw.get("wind_dir")) if gw else None,
                "precip_prob": _safe_float(gw.get("precip_prob")) if gw else None,
                "conditions": gw.get("conditions") if gw else None,
                "ballpark_name": gw.get("ballpark_name") if gw else None,
                "roof_type": gw.get("roof_type") if gw else None,
                # Odds fields
                "home_moneyline": _safe_int(gw.get("home_moneyline")) if gw else None,
                "away_moneyline": _safe_int(gw.get("away_moneyline")) if gw else None,
                "over_under": _safe_float(gw.get("over_under")) if gw else None,
                "weather_note": gw.get("weather_note") if gw else None,
                "ballpark_azimuth": _safe_int(gw.get("ballpark_azimuth")) if gw else None,
            }
        )
    return rows


@router.get("/mlb/matchups/upcoming/debug")
def mlb_matchups_upcoming_debug():
    now_et = datetime.now(NY_TZ)
    today = now_et.date()
    tomorrow = today + timedelta(days=1)
    today_iso = today.isoformat()
    tomorrow_iso = tomorrow.isoformat()

    today_debug = _fetch_schedule_for_date_iso_debug(today_iso)
    tomorrow_debug = _fetch_schedule_for_date_iso_debug(tomorrow_iso)

    schedule_rows = _fetch_schedule_for_today()
    game_pks = [row.get("game_pk") for row in schedule_rows]

    bq_status: Dict[str, Any]
    try:
        client = get_bq_client()
        hr_table = _qualified_table(client, HR_PICKS_TABLE)
        bq_rows = _safe_query(
            client,
            f"""
            SELECT COUNT(*) AS row_count
            FROM {hr_table}
            WHERE run_date = @run_date
            """,
            [bigquery.ScalarQueryParameter("run_date", "DATE", _today_et_iso())],
        )
        row_count = int(bq_rows[0]["row_count"]) if bq_rows else 0
        bq_status = {"ok": True, "today_row_count": row_count}
    except Exception as exc:
        bq_status = {"ok": False, "error": _exception_message(exc), "today_row_count": 0}

    return {
        "now_et": now_et.isoformat(),
        "today_et": today_iso,
        "tomorrow_et": tomorrow_iso,
        "schedule_today": today_debug,
        "schedule_tomorrow": tomorrow_debug,
        "combined_schedule_rows": len(schedule_rows),
        "combined_game_pks": game_pks[:20],
        "bq_status": bq_status,
        "upcoming_endpoint_return_count": len(mlb_matchups_upcoming(limit=20)),
    }


@router.get("/mlb/matchups/{game_pk}")
def mlb_matchup_detail(game_pk: int):
    client = get_bq_client()
    today = _today_et_iso()
    hr_table = _qualified_table(client, HR_PICKS_TABLE)
    pitcher_table = _qualified_table(client, PITCHER_MATCHUP_TABLE)
    pitch_log_table = _qualified_table(client, PITCH_LOG_TABLE)
    hit_data_table = _qualified_table(client, HIT_DATA_TABLE)
    weather_table = _qualified_table(client, GAME_WEATHER_TABLE)
    schedule = _fetch_schedule_for_game(game_pk)
    home_team = schedule.get("home_team") if schedule else None
    away_team = schedule.get("away_team") if schedule else None
    home_team_id = _safe_int(schedule.get("home_team_id")) if schedule else None
    away_team_id = _safe_int(schedule.get("away_team_id")) if schedule else None
    run_date = _resolve_latest_run_date_for_game(client, hr_table, game_pk, today)

    # Run weather, pitcher splits, and picks in parallel
    def _get_weather():
        return _fetch_game_weather_map(client, weather_table, run_date, [game_pk])

    def _get_pitcher_splits():
        return _safe_query(
            client,
            f"""
            SELECT
              CAST(game_pk AS INT64) AS game_pk,
              pitcher_id, pitcher_name, pitcher_hand, opp_team_id, split,
              ip, home_runs, hr_per_9, barrel_pct, hard_hit_pct,
              fb_pct, hr_fb_pct, whip, woba
            FROM {pitcher_table}
            WHERE run_date = @run_date
              AND CAST(game_pk AS INT64) = @game_pk
              AND split IN ('Season', 'vsLHB', 'vsRHB')
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY pitcher_id, split
              ORDER BY ingested_at DESC NULLS LAST
            ) = 1
            ORDER BY pitcher_name ASC, split ASC
            """,
            [
                bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
            ],
        )

    def _get_picks():
        return _safe_query(
            client,
            f"""
            SELECT
              CAST(game_pk AS INT64) AS game_pk,
              batter_id, batter_name, bat_side,
              pitcher_id, pitcher_name, pitcher_hand,
              score, grade, why, flags,
              iso, slg, l15_ev, l15_barrel_pct,
              season_ev, season_barrel_pct, l15_hard_hit_pct, hr_fb_pct,
              p_hr9_vs_hand, p_hr_fb_pct, p_barrel_pct, p_fb_pct,
              p_hard_hit_pct, p_iso_allowed,
              weather_indicator, game_temp, wind_speed, wind_dir,
              precip_prob, ballpark_name, roof_type, weather_note,
              home_moneyline, away_moneyline, over_under,
              hr_odds_best_price, hr_odds_best_book,
              deep_link_desktop, deep_link_ios,
              dk_outcome_code, dk_event_id, fd_market_id, fd_selection_id
            FROM {hr_table}
            WHERE run_date = @run_date
              AND CAST(game_pk AS INT64) = @game_pk
            ORDER BY score DESC, batter_name ASC
            """,
            [
                bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
            ],
        )

    with ThreadPoolExecutor(max_workers=3) as pool:
        fut_weather = pool.submit(_get_weather)
        fut_splits = pool.submit(_get_pitcher_splits)
        fut_picks = pool.submit(_get_picks)

    weather_map = fut_weather.result()
    gw = weather_map.get(game_pk)
    pitcher_splits = fut_splits.result()
    picks = fut_picks.result()

    if not picks:
        pitcher_ids = sorted({_safe_int(row.get("pitcher_id")) for row in pitcher_splits if _safe_int(row.get("pitcher_id")) is not None})
        pitcher_names = sorted({_normalized_name(row.get("pitcher_name")) for row in pitcher_splits if _normalized_name(row.get("pitcher_name"))})
        team_names = sorted({_normalized_name(name) for name in [home_team, away_team] if _normalized_name(name)})

        if pitcher_ids or pitcher_names:
            picks = _safe_query(
                client,
                f"""
                SELECT
                  CAST(game_pk AS INT64) AS game_pk,
                  batter_id,
                  batter_name,
                  bat_side,
                  pitcher_id,
                  pitcher_name,
                  pitcher_hand,
                  score,
                  grade,
                  why,
                  flags,
                  iso,
                  slg,
                  l15_ev,
                  l15_barrel_pct,
                  season_ev,
                  season_barrel_pct,
                  l15_hard_hit_pct,
                  hr_fb_pct,
                  p_hr9_vs_hand,
                  p_hr_fb_pct,
                  p_barrel_pct,
                  p_fb_pct,
                  p_hard_hit_pct,
                  p_iso_allowed,
                  weather_indicator,
                  game_temp,
                  wind_speed,
                  wind_dir,
                  precip_prob,
                  ballpark_name,
                  roof_type,
                  weather_note,
                  home_moneyline,
                  away_moneyline,
                  over_under,
                  hr_odds_best_price,
                  hr_odds_best_book,
                  deep_link_desktop,
                  deep_link_ios,
                  dk_outcome_code,
                  dk_event_id,
                  fd_market_id,
                  fd_selection_id
                FROM {hr_table}
                WHERE run_date = @run_date
                  AND (
                    (ARRAY_LENGTH(@pitcher_ids) > 0 AND CAST(pitcher_id AS INT64) IN UNNEST(@pitcher_ids))
                    OR (ARRAY_LENGTH(@pitcher_names) > 0 AND LOWER(CAST(pitcher_name AS STRING)) IN UNNEST(@pitcher_names))
                  )
                  AND (
                    ARRAY_LENGTH(@team_names) = 0
                    OR LOWER(CAST(home_team AS STRING)) IN UNNEST(@team_names)
                    OR LOWER(CAST(away_team AS STRING)) IN UNNEST(@team_names)
                  )
                ORDER BY score DESC, batter_name ASC
                """,
                [
                    bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                    bigquery.ArrayQueryParameter("pitcher_ids", "INT64", pitcher_ids),
                    bigquery.ArrayQueryParameter("pitcher_names", "STRING", pitcher_names),
                    bigquery.ArrayQueryParameter("team_names", "STRING", team_names),
                ],
            )

    # Backfill sportsbook/deep-link fields from live /mlb/props when BQ rows miss them.
    picks = _merge_props_fallback_into_picks(picks, game_pk)

    pitcher_ids_for_pitch_tables = sorted(
        {
            pid
            for pid in (
                _safe_int(row.get("pitcher_id")) for row in pitcher_splits
            )
            if pid is not None
        }
        | {
            pid
            for pid in (
                _safe_int(row.get("pitcher_id")) for row in picks
            )
            if pid is not None
        }
    )
    batter_ids_for_pitch_tables = sorted(
        {
            bid
            for bid in (
                _safe_int(row.get("batter_id")) for row in picks
            )
            if bid is not None
        }
    )
    # Run independent BQ queries in parallel
    with ThreadPoolExecutor(max_workers=3) as pool:
        fut_mix = pool.submit(
            _fetch_pitcher_pitch_mix_map,
            client, pitch_log_table, hit_data_table,
            run_date, game_pk, pitcher_ids_for_pitch_tables,
        )
        fut_bvp_pitches = pool.submit(
            _fetch_batter_vs_pitches_map,
            client, hit_data_table,
            run_date, game_pk, batter_ids_for_pitch_tables,
        )
        fut_bvp_career = pool.submit(
            _fetch_bvp_career_map,
            client, hit_data_table,
            run_date, game_pk,
            batter_ids_for_pitch_tables, pitcher_ids_for_pitch_tables,
        )

    pitcher_pitch_mix_map = fut_mix.result()
    batter_vs_pitches_map = fut_bvp_pitches.result()
    bvp_career_map = fut_bvp_career.result()

    grade_counts = {"IDEAL": 0, "FAVORABLE": 0, "AVERAGE": 0, "AVOID": 0}
    pitcher_groups: Dict[str, Dict[str, Any]] = {}

    for split_row in pitcher_splits:
        pitcher_id = _safe_int(split_row.get("pitcher_id"))
        group_key = _pitcher_group_key(pitcher_id, split_row.get("pitcher_name"))
        if group_key is None:
            continue
        group = pitcher_groups.setdefault(
            group_key,
            {
                "pitcher_id": pitcher_id,
                "pitcher_name": split_row.get("pitcher_name"),
                "pitcher_hand": split_row.get("pitcher_hand"),
                "opp_team_id": _safe_int(split_row.get("opp_team_id")),
                "splits": {},
                "batters": [],
            },
        )
        if group.get("pitcher_id") is None and pitcher_id is not None:
            group["pitcher_id"] = pitcher_id
        if not group.get("pitcher_name") and split_row.get("pitcher_name"):
            group["pitcher_name"] = split_row.get("pitcher_name")
        if not group.get("pitcher_hand") and split_row.get("pitcher_hand"):
            group["pitcher_hand"] = split_row.get("pitcher_hand")
        if group.get("opp_team_id") is None and _safe_int(split_row.get("opp_team_id")) is not None:
            group["opp_team_id"] = _safe_int(split_row.get("opp_team_id"))
        split_name = split_row.get("split") or "Season"
        group["splits"][split_name] = {
            "ip": _safe_float(split_row.get("ip")),
            "home_runs": _safe_int(split_row.get("home_runs")),
            "hr_per_9": _safe_float(split_row.get("hr_per_9")),
            "barrel_pct": _safe_float(split_row.get("barrel_pct")),
            "hard_hit_pct": _safe_float(split_row.get("hard_hit_pct")),
            "fb_pct": _safe_float(split_row.get("fb_pct")),
            "hr_fb_pct": _safe_float(split_row.get("hr_fb_pct")),
            "whip": _safe_float(split_row.get("whip")),
            "woba": _safe_float(split_row.get("woba")),
        }

    for pick in picks:
        pitcher_id = _safe_int(pick.get("pitcher_id"))
        batter_id = _safe_int(pick.get("batter_id"))
        group_key = _pitcher_group_key(pitcher_id, pick.get("pitcher_name"))
        if group_key is None:
            continue
        group = pitcher_groups.setdefault(
            group_key,
            {
                "pitcher_id": pitcher_id,
                "pitcher_name": pick.get("pitcher_name"),
                "pitcher_hand": pick.get("pitcher_hand"),
                "opp_team_id": None,
                "splits": {},
                "batters": [],
            },
        )
        if group.get("pitcher_id") is None and pitcher_id is not None:
            group["pitcher_id"] = pitcher_id
        if not group.get("pitcher_name") and pick.get("pitcher_name"):
            group["pitcher_name"] = pick.get("pitcher_name")
        if not group.get("pitcher_hand") and pick.get("pitcher_hand"):
            group["pitcher_hand"] = pick.get("pitcher_hand")
        grade = (pick.get("grade") or "").upper()
        if grade in grade_counts:
            grade_counts[grade] += 1
        pitcher_mix_rows = pitcher_pitch_mix_map.get(pitcher_id or -1, {"L": [], "R": []})
        batter_vs_rows = batter_vs_pitches_map.get(batter_id or -1, {"L": [], "R": []})
        group["batters"].append(
            {
                "batter_id": batter_id,
                "batter_name": pick.get("batter_name"),
                "bat_side": pick.get("bat_side"),
                "score": _safe_float(pick.get("score")),
                "grade": pick.get("grade"),
                "why": pick.get("why"),
                "flags": _parse_flags(pick.get("flags")),
                "iso": _safe_float(pick.get("iso")),
                "slg": _safe_float(pick.get("slg")),
                "l15_ev": _safe_float(pick.get("l15_ev")),
                "l15_barrel_pct": _safe_float(pick.get("l15_barrel_pct")),
                "season_ev": _safe_float(pick.get("season_ev")),
                "season_barrel_pct": _safe_float(pick.get("season_barrel_pct")),
                "l15_hard_hit_pct": _safe_float(pick.get("l15_hard_hit_pct")),
                "hr_fb_pct": _safe_float(pick.get("hr_fb_pct")),
                "p_hr9_vs_hand": _safe_float(pick.get("p_hr9_vs_hand")),
                "p_hr_fb_pct": _safe_float(pick.get("p_hr_fb_pct")),
                "p_barrel_pct": _safe_float(pick.get("p_barrel_pct")),
                "p_fb_pct": _safe_float(pick.get("p_fb_pct")),
                "p_hard_hit_pct": _safe_float(pick.get("p_hard_hit_pct")),
                "p_iso_allowed": _safe_float(pick.get("p_iso_allowed")),
                "weather_indicator": _clean_str(pick.get("weather_indicator")),
                "game_temp": _safe_float(pick.get("game_temp")),
                "wind_speed": _safe_float(pick.get("wind_speed")),
                "wind_dir": _safe_int(pick.get("wind_dir")),
                "wind_direction_label": _wind_direction_label(_safe_int(pick.get("wind_dir"))),
                "precip_prob": _safe_float(pick.get("precip_prob")),
                "ballpark_name": _clean_str(pick.get("ballpark_name")),
                "roof_type": _clean_str(pick.get("roof_type")),
                "weather_note": _clean_str(pick.get("weather_note")),
                "home_moneyline": _safe_int(pick.get("home_moneyline")),
                "away_moneyline": _safe_int(pick.get("away_moneyline")),
                "over_under": _safe_float(pick.get("over_under")),
                "hr_odds_best_price": _safe_int(pick.get("hr_odds_best_price")),
                "hr_odds_best_book": _clean_str(pick.get("hr_odds_best_book")),
                "deep_link_desktop": _clean_str(pick.get("deep_link_desktop")),
                "deep_link_ios": _clean_str(pick.get("deep_link_ios")),
                "dk_outcome_code": _clean_str(pick.get("dk_outcome_code")),
                "dk_event_id": _clean_str(pick.get("dk_event_id")),
                "fd_market_id": _clean_str(pick.get("fd_market_id")),
                "fd_selection_id": _clean_str(pick.get("fd_selection_id")),
                "pitcher_pitch_mix": {
                    "vs_lhb": list(pitcher_mix_rows.get("L", [])),
                    "vs_rhb": list(pitcher_mix_rows.get("R", [])),
                },
                "hitter_stats_vs_pitches": {
                    "vs_lhp": list(batter_vs_rows.get("L", [])),
                    "vs_rhp": list(batter_vs_rows.get("R", [])),
                },
                "bvp_career": bvp_career_map.get(
                    f"{batter_id}:{pitcher_id}", None
                ),
            }
        )

    pitchers_out: List[Dict[str, Any]] = []
    for pitcher in pitcher_groups.values():
        offense_team = None
        opp_team_id = pitcher.get("opp_team_id")
        if opp_team_id is not None:
            if home_team_id is not None and opp_team_id == home_team_id:
                offense_team = home_team
            elif away_team_id is not None and opp_team_id == away_team_id:
                offense_team = away_team

        pitcher["offense_team"] = offense_team
        pitcher["batters"] = sorted(
            pitcher.get("batters", []),
            key=lambda row: row.get("score") or 0,
            reverse=True,
        )
        pitchers_out.append(pitcher)

    pitchers_out.sort(
        key=lambda row: (row["batters"][0].get("score") or 0) if row.get("batters") else 0,
        reverse=True,
    )

    all_batters = [b for pitcher in pitchers_out for b in pitcher.get("batters", [])]
    game_weather = {
        "weather_indicator": _first_present(*(b.get("weather_indicator") for b in all_batters)),
        "game_temp": _first_present(*(b.get("game_temp") for b in all_batters)),
        "wind_speed": _first_present(*(b.get("wind_speed") for b in all_batters)),
        "wind_dir": _first_present(*(b.get("wind_dir") for b in all_batters)),
        "precip_prob": _first_present(*(b.get("precip_prob") for b in all_batters)),
        "ballpark_name": _first_present(*(b.get("ballpark_name") for b in all_batters)),
        "roof_type": _first_present(*(b.get("roof_type") for b in all_batters)),
        "weather_note": _first_present(*(b.get("weather_note") for b in all_batters)),
    }
    game_odds = {
        "home_moneyline": _first_present(*(b.get("home_moneyline") for b in all_batters)),
        "away_moneyline": _first_present(*(b.get("away_moneyline") for b in all_batters)),
        "over_under": _first_present(*(b.get("over_under") for b in all_batters)),
    }
    wind_dir = _safe_int(game_weather.get("wind_dir"))
    game_weather["wind_dir"] = wind_dir
    game_weather["wind_direction_label"] = _wind_direction_label(wind_dir)

    return {
        "game_pk": game_pk,
        "run_date": run_date,
        "game": {
            "home_team": home_team,
            "away_team": away_team,
            "start_time_utc": schedule.get("start_time_utc") if schedule else None,
            "venue_name": gw.get("ballpark_name") if gw else (schedule.get("venue_name") if schedule else None),
            "home_pitcher_name": schedule.get("home_pitcher_name") if schedule else None,
            "away_pitcher_name": schedule.get("away_pitcher_name") if schedule else None,
            "weather": game_weather,
            "odds": game_odds,
        },
        "grade_counts": grade_counts,
        "pitchers": pitchers_out,
    }


# ── Pitching Props (Strikeout) endpoint ─────────────────────────────────────

def _fetch_k_props_live(game_pk: int) -> List[Dict[str, Any]]:
    """Fetch all pitching_strikeouts props (standard + alt) from propfinder API."""
    try:
        url = f"{PROPFINDER_MLB_PROPS_URL}?gameId={game_pk}"
        request = Request(url, headers={"User-Agent": "PulseSports/1.0", "Accept": "application/json"})
        with urlopen(request, timeout=15) as response:
            data = json.loads(response.read().decode("utf-8"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []

    out: List[Dict[str, Any]] = []
    for item in data:
        if item.get("category") != "pitching_strikeouts":
            continue
        if item.get("overUnder") != "over":
            continue

        # Extract DK and FD deep links from markets list
        dk_desktop = dk_ios = dk_price = dk_outcome = dk_event = None
        fd_desktop = fd_ios = fd_price = fd_market = fd_selection = None
        for m in item.get("markets", []):
            book = m.get("sportsbook", "")
            if book == "DraftKings" and dk_desktop is None:
                dk_price = _safe_int(m.get("price"))
                dk_desktop = m.get("deepLinkDesktop") or None
                dk_ios = m.get("deepLinkIos") or None
                # Parse DK outcome code from desktop URL
                import re
                match = re.search(r"outcomes=([^&\s]+)", dk_desktop or "")
                if match:
                    dk_outcome = match.group(1)
                match2 = re.search(r"/event/(\d+)", dk_desktop or "")
                if match2:
                    dk_event = match2.group(1)
            if book == "FanDuel" and fd_desktop is None:
                fd_price = _safe_int(m.get("price"))
                fd_desktop = m.get("deepLinkDesktop") or None
                fd_ios = m.get("deepLinkIos") or None
                from urllib.parse import urlparse, parse_qs as _pqs
                parsed = urlparse(fd_desktop or "")
                qs = _pqs(parsed.query)
                fd_market = (qs.get("marketId") or [None])[0]
                fd_selection = (qs.get("selectionId") or [None])[0]

        best = item.get("bestMarket") or {}
        out.append({
            "pitcher_id": _safe_int(item.get("playerId")),
            "pitcher_name": item.get("name"),
            "team_code": item.get("teamCode"),
            "opp_team_code": item.get("opposingTeamCode"),
            "line": _safe_float(item.get("line")),
            "is_alternate": item.get("isAlternate") is not None,
            "is_standard": item.get("isAlternate") is None,
            "best_price": _safe_int(best.get("price")),
            "best_book": best.get("sportsbook"),
            "pf_rating": _safe_float(item.get("pfRating")),
            "hit_rate_l5": item.get("hitRateL5"),
            "hit_rate_l10": item.get("hitRateL10"),
            "hit_rate_season": item.get("hitRateSeason"),
            "hit_rate_vs_team": item.get("hitRateVsTeam"),
            "avg_l10": _safe_float(item.get("avgL10")),
            "avg_home_away": _safe_float(item.get("avgHomeAway")),
            "avg_vs_opponent": _safe_float(item.get("avgVsOpponent")),
            "streak": _safe_int(item.get("streak")),
            # DraftKings
            "dk_price": dk_price,
            "dk_outcome_code": dk_outcome,
            "dk_event_id": dk_event,
            "dk_desktop": dk_desktop,
            "dk_ios": dk_ios,
            # FanDuel
            "fd_price": fd_price,
            "fd_market_id": fd_market,
            "fd_selection_id": fd_selection,
            "fd_desktop": fd_desktop,
            "fd_ios": fd_ios,
        })
    return out


@router.get("/mlb/matchups/{game_pk}/pitching-props")
def mlb_pitching_props(game_pk: int):
    """
    Returns pitching K prop grades for a game.
    Combines BQ signal data with live sportsbook lines + deep links.
    """
    client = get_bq_client()
    today = _today_et_iso()
    grades_view = _qualified_table(client, K_GRADES_VIEW)
    signal_view = _qualified_table(client, K_SIGNAL_VIEW)
    team_k_table = _qualified_table(client, TEAM_K_RANKINGS_TABLE)
    weather_table = _qualified_table(client, GAME_WEATHER_TABLE)

    schedule = _fetch_schedule_for_game(game_pk)
    home_team = schedule.get("home_team") if schedule else None
    away_team = schedule.get("away_team") if schedule else None
    home_team_id = _safe_int(schedule.get("home_team_id")) if schedule else None
    away_team_id = _safe_int(schedule.get("away_team_id")) if schedule else None

    # Fetch weather/odds for game context
    weather_map = _fetch_game_weather_map(client, weather_table, today, [game_pk])
    gw = weather_map.get(game_pk) or {}

    # 1. Fetch K signal data from BQ view (for this game's pitchers)
    signal_rows = _safe_query(
        client,
        f"""
        SELECT *
        FROM {signal_view}
        WHERE game_pk = @game_pk
        ORDER BY k_signal_rank ASC
        """,
        [bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk)],
    )

    # 2. Fetch opposing team K rankings
    opp_team_ids = sorted({
        _safe_int(r.get("opp_team_id"))
        for r in signal_rows
        if _safe_int(r.get("opp_team_id")) is not None
    })
    team_k_rows = _safe_query(
        client,
        f"""
        SELECT team_id, team_name, split, rank, value
        FROM {team_k_table}
        WHERE run_date = @run_date
          AND category = 'strikeouts'
          AND team_id IN UNNEST(@team_ids)
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY team_id, split
          ORDER BY ingested_at DESC
        ) = 1
        """,
        [
            bigquery.ScalarQueryParameter("run_date", "DATE", today),
            bigquery.ArrayQueryParameter("team_ids", "INT64", opp_team_ids if opp_team_ids else [0]),
        ],
    ) if opp_team_ids else []

    # Build team K map: team_id -> { split -> { rank, value } }
    team_k_map: Dict[int, Dict[str, Any]] = {}
    for row in team_k_rows:
        tid = _safe_int(row.get("team_id"))
        if tid is None:
            continue
        entry = team_k_map.setdefault(tid, {"team_name": row.get("team_name"), "splits": {}})
        entry["splits"][row.get("split", "Season")] = {
            "rank": _safe_int(row.get("rank")),
            "value": _safe_int(row.get("value")),
        }

    # 3. Fetch live K prop lines with DK/FD deep links
    live_props = _fetch_k_props_live(game_pk)

    # Index live props by pitcher_id
    props_by_pitcher: Dict[int, Dict[str, Any]] = {}
    for prop in live_props:
        pid = prop.get("pitcher_id")
        if pid is None:
            continue
        entry = props_by_pitcher.setdefault(pid, {"standard": None, "alt_lines": []})
        if prop.get("is_standard"):
            entry["standard"] = prop
        else:
            entry["alt_lines"].append(prop)

    # 4. Assemble pitcher-level response
    pitchers_out: List[Dict[str, Any]] = []
    for sig in signal_rows:
        pid = _safe_int(sig.get("pitcher_id"))
        opp_tid = _safe_int(sig.get("opp_team_id"))
        pitcher_hand = sig.get("pitcher_hand")

        # Determine which team this pitcher faces (offense team)
        offense_team = None
        if opp_tid is not None:
            if home_team_id is not None and opp_tid == home_team_id:
                offense_team = home_team
            elif away_team_id is not None and opp_tid == away_team_id:
                offense_team = away_team

        # Get live prop data
        prop_data = props_by_pitcher.get(pid, {})
        standard = prop_data.get("standard") or {}
        alt_lines_raw = prop_data.get("alt_lines") or []

        # Filter alt lines to those near the projection (±2 from proj_ks)
        proj_ks = _safe_float(sig.get("proj_ks")) or 0
        alt_lines = sorted(
            [
                {
                    "line": a.get("line"),
                    "best_price": a.get("best_price"),
                    "best_book": a.get("best_book"),
                    "pf_rating": a.get("pf_rating"),
                    "dk_price": a.get("dk_price"),
                    "dk_outcome_code": a.get("dk_outcome_code"),
                    "dk_event_id": a.get("dk_event_id"),
                    "dk_desktop": a.get("dk_desktop"),
                    "dk_ios": a.get("dk_ios"),
                    "fd_price": a.get("fd_price"),
                    "fd_market_id": a.get("fd_market_id"),
                    "fd_selection_id": a.get("fd_selection_id"),
                    "fd_desktop": a.get("fd_desktop"),
                    "fd_ios": a.get("fd_ios"),
                }
                for a in alt_lines_raw
                if a.get("line") is not None
            ],
            key=lambda x: x.get("line") or 0,
        )

        # Team K vulnerability
        opp_k = team_k_map.get(opp_tid, {}) if opp_tid else {}

        pitchers_out.append({
            "pitcher_id": pid,
            "pitcher_name": sig.get("pitcher_name"),
            "pitcher_hand": pitcher_hand,
            "offense_team": offense_team,
            "team_code": standard.get("team_code"),
            "opp_team_code": standard.get("opp_team_code"),

            # Signal / projection
            "k_signal_score": _safe_float(sig.get("k_signal_score")),
            "k_signal_rank": _safe_int(sig.get("k_signal_rank")),
            "proj_ks": _safe_float(sig.get("proj_ks")),
            "proj_ip": _safe_float(sig.get("proj_ip")),
            "proj_outs": _safe_int(sig.get("proj_outs")),

            # Pitcher K stats
            "ip": _safe_float(sig.get("ip")),
            "strikeouts": _safe_int(sig.get("strikeouts")),
            "strikeouts_per_9": _safe_float(sig.get("strikeouts_per_9")),
            "k_pct": _safe_float(sig.get("k_pct")),
            "strike_pct": _safe_float(sig.get("strike_pct")),
            "strikeout_walk_ratio": _safe_float(sig.get("strikeout_walk_ratio")),
            "batters_faced": _safe_int(sig.get("batters_faced")),
            "whip": _safe_float(sig.get("whip")),
            "woba": _safe_float(sig.get("woba")),

            # Pitcher vs-hand
            "hand_split": sig.get("hand_split"),
            "hand_k_per_9": _safe_float(sig.get("hand_k_per_9")),
            "hand_k_pct": _safe_float(sig.get("hand_k_pct")),

            # Arsenal
            "arsenal_whiff_rate": _safe_float(sig.get("arsenal_whiff_rate")),
            "arsenal_k_pct": _safe_float(sig.get("arsenal_k_pct")),
            "max_pitch_whiff": _safe_float(sig.get("max_pitch_whiff")),
            "pitch_type_count": _safe_int(sig.get("pitch_type_count")),

            # Team K adjustment
            "team_k_adj": _safe_float(sig.get("team_k_adj")),

            # Opposing team K vulnerability
            "opp_team_k": {
                "team_name": opp_k.get("team_name"),
                "splits": opp_k.get("splits", {}),
            },

            # Standard line
            "k_line": _safe_float(standard.get("line")),
            "k_best_price": _safe_int(standard.get("best_price")),
            "k_best_book": standard.get("best_book"),
            "pf_rating": _safe_float(standard.get("pf_rating")),
            "hit_rate_l5": standard.get("hit_rate_l5"),
            "hit_rate_l10": standard.get("hit_rate_l10"),
            "hit_rate_season": standard.get("hit_rate_season"),
            "hit_rate_vs_team": standard.get("hit_rate_vs_team"),
            "avg_l10": _safe_float(standard.get("avg_l10")),
            "avg_home_away": _safe_float(standard.get("avg_home_away")),
            "avg_vs_opponent": _safe_float(standard.get("avg_vs_opponent")),
            "streak": _safe_int(standard.get("streak")),

            # Edge / grade
            "edge": round(proj_ks - (standard.get("line") or 0), 1) if standard.get("line") else None,
            "over_grade": _k_grade(proj_ks, _safe_float(standard.get("line"))),
            "lean": _k_lean(proj_ks, _safe_float(standard.get("line"))),
            "confidence": _k_confidence(proj_ks, _safe_float(standard.get("line")), _safe_float(sig.get("k_signal_score"))),

            # DK/FD for standard line
            "dk_price": _safe_int(standard.get("dk_price")),
            "dk_outcome_code": standard.get("dk_outcome_code"),
            "dk_event_id": standard.get("dk_event_id"),
            "dk_desktop": standard.get("dk_desktop"),
            "dk_ios": standard.get("dk_ios"),
            "fd_price": _safe_int(standard.get("fd_price")),
            "fd_market_id": standard.get("fd_market_id"),
            "fd_selection_id": standard.get("fd_selection_id"),
            "fd_desktop": standard.get("fd_desktop"),
            "fd_ios": standard.get("fd_ios"),

            # Alt lines
            "alt_lines": alt_lines,
        })

    game_weather = {
        "weather_indicator": gw.get("weather_indicator"),
        "game_temp": _safe_float(gw.get("game_temp")),
        "wind_speed": _safe_float(gw.get("wind_speed")),
        "wind_dir": _safe_int(gw.get("wind_dir")),
        "wind_direction_label": _wind_direction_label(_safe_int(gw.get("wind_dir"))),
        "precip_prob": _safe_float(gw.get("precip_prob")),
        "ballpark_name": _clean_str(gw.get("ballpark_name")),
        "roof_type": _clean_str(gw.get("roof_type")),
        "weather_note": _clean_str(gw.get("weather_note")),
    }
    game_odds = {
        "home_moneyline": _safe_int(gw.get("home_moneyline")),
        "away_moneyline": _safe_int(gw.get("away_moneyline")),
        "over_under": _safe_float(gw.get("over_under")),
    }

    return {
        "game_pk": game_pk,
        "run_date": today,
        "game": {
            "home_team": home_team,
            "away_team": away_team,
            "start_time_utc": schedule.get("start_time_utc") if schedule else None,
            "venue_name": gw.get("ballpark_name") or (schedule.get("venue_name") if schedule else None),
            "home_pitcher_name": schedule.get("home_pitcher_name") if schedule else None,
            "away_pitcher_name": schedule.get("away_pitcher_name") if schedule else None,
            "weather": game_weather,
            "odds": game_odds,
        },
        "pitchers": pitchers_out,
    }


def _k_grade(proj_ks: float, line: Optional[float]) -> str:
    if line is None:
        return "N/A"
    diff = proj_ks - line
    if diff >= 2.0:
        return "A+"
    if diff >= 1.5:
        return "A"
    if diff >= 1.0:
        return "B+"
    if diff >= 0.5:
        return "B"
    if diff >= 0.0:
        return "C+"
    if diff >= -0.5:
        return "C"
    if diff >= -1.0:
        return "C-"
    if diff >= -1.5:
        return "D"
    return "F"


def _k_lean(proj_ks: float, line: Optional[float]) -> str:
    if line is None:
        return "N/A"
    diff = proj_ks - line
    if diff >= 0.5:
        return "OVER"
    if diff <= -0.5:
        return "UNDER"
    return "PASS"


def _k_confidence(proj_ks: float, line: Optional[float], signal_score: Optional[float]) -> str:
    if line is None:
        return "LOW"
    edge = abs(proj_ks - line)
    score = signal_score or 0
    if edge >= 1.0 and score >= 50:
        return "HIGH"
    if edge >= 0.5 and score >= 35:
        return "MEDIUM"
    return "LOW"


# ── Batting Order Matchup endpoint ──────────────────────────────────────────

PROPFINDER_UPCOMING_URL = os.getenv(
    "PROPFINDER_UPCOMING_URL", "https://api.propfinder.app/mlb/upcoming-games"
)
MLB_PEOPLE_URL = "https://statsapi.mlb.com/api/v1/people"

# OPS threshold for "weak spot" — positions where pitcher is historically exploitable
WEAK_SPOT_OPS_THRESHOLD = 0.780


def _fetch_lineup_for_game(game_pk: int) -> Optional[Dict[str, Any]]:
    """Fetch lineup data from propfinder upcoming-games, filtered to game_pk."""
    try:
        request = Request(
            PROPFINDER_UPCOMING_URL,
            headers={"User-Agent": "PulseSports/1.0", "Accept": "application/json"},
        )
        with urlopen(request, timeout=15) as response:
            data = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None
    if not isinstance(data, list):
        return None
    for game in data:
        if _safe_int(game.get("id")) == game_pk:
            return game
    return None


def _fetch_player_names_bulk(player_ids: List[int]) -> Dict[int, str]:
    """Batch-fetch player fullNames from MLB Stats API."""
    if not player_ids:
        return {}
    ids_str = ",".join(str(pid) for pid in player_ids)
    try:
        url = f"{MLB_PEOPLE_URL}?personIds={ids_str}&fields=people,id,fullName"
        request = Request(
            url,
            headers={"User-Agent": "PulseSports/1.0", "Accept": "application/json"},
        )
        with urlopen(request, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
    except Exception:
        return {}
    return {
        int(p["id"]): p.get("fullName", "")
        for p in data.get("people", [])
        if p.get("id")
    }


def _is_weak_spot(ops: Optional[float]) -> bool:
    return ops is not None and ops >= WEAK_SPOT_OPS_THRESHOLD


@router.get("/mlb/matchups/{game_pk}/batting-order")
def mlb_batting_order(game_pk: int):
    """
    Returns pitcher vs batting order position stats with confirmed lineup
    player names mapped to each position. Flags weak spots.
    """
    client = get_bq_client()
    today = _today_et_iso()
    bo_table = _qualified_table(client, BATTING_ORDER_TABLE)
    weather_table = _qualified_table(client, GAME_WEATHER_TABLE)

    schedule = _fetch_schedule_for_game(game_pk)
    home_team = schedule.get("home_team") if schedule else None
    away_team = schedule.get("away_team") if schedule else None
    home_team_id = _safe_int(schedule.get("home_team_id")) if schedule else None
    away_team_id = _safe_int(schedule.get("away_team_id")) if schedule else None

    weather_map = _fetch_game_weather_map(client, weather_table, today, [game_pk])
    gw = weather_map.get(game_pk) or {}

    # 1. Fetch batting order stats from BQ
    bo_rows = _safe_query(
        client,
        f"""
        SELECT
          pitcher_id, pitcher_name, pitcher_hand, opp_team_id,
          batting_order, at_bats, hits, home_runs, doubles, triples,
          rbi, walks, strike_outs, avg, obp, slg, ops
        FROM {bo_table}
        WHERE run_date = @run_date
          AND game_pk = @game_pk
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY pitcher_id, batting_order
          ORDER BY ingested_at DESC
        ) = 1
        ORDER BY pitcher_id, batting_order
        """,
        [
            bigquery.ScalarQueryParameter("run_date", "DATE", today),
            bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
        ],
    )

    # 2. Fetch lineup from propfinder
    lineup_data = _fetch_lineup_for_game(game_pk)
    home_order_str = (lineup_data or {}).get("homeBattingOrder", "")
    away_order_str = (lineup_data or {}).get("visitorBattingOrder", "")
    home_lineup = [int(x) for x in home_order_str.split(",") if x.strip().isdigit()]
    away_lineup = [int(x) for x in away_order_str.split(",") if x.strip().isdigit()]

    # 3. Fetch player names
    all_player_ids = list(set(home_lineup + away_lineup))
    name_map = _fetch_player_names_bulk(all_player_ids)

    # 4. Group by pitcher
    pitcher_map: Dict[int, Dict[str, Any]] = {}
    for row in bo_rows:
        pid = _safe_int(row.get("pitcher_id"))
        if pid is None:
            continue
        group = pitcher_map.setdefault(pid, {
            "pitcher_id": pid,
            "pitcher_name": row.get("pitcher_name"),
            "pitcher_hand": row.get("pitcher_hand"),
            "opp_team_id": _safe_int(row.get("opp_team_id")),
            "positions": [],
        })
        ops_val = _safe_float(row.get("ops"))
        group["positions"].append({
            "batting_order": _safe_int(row.get("batting_order")),
            "at_bats": _safe_int(row.get("at_bats")),
            "hits": _safe_int(row.get("hits")),
            "home_runs": _safe_int(row.get("home_runs")),
            "doubles": _safe_int(row.get("doubles")),
            "triples": _safe_int(row.get("triples")),
            "rbi": _safe_int(row.get("rbi")),
            "walks": _safe_int(row.get("walks")),
            "strike_outs": _safe_int(row.get("strike_outs")),
            "avg": _safe_float(row.get("avg")),
            "obp": _safe_float(row.get("obp")),
            "slg": _safe_float(row.get("slg")),
            "ops": ops_val,
            "is_weak_spot": _is_weak_spot(ops_val),
            "player_id": None,
            "player_name": None,
        })

    # 5. Map lineups to positions
    pitchers_out: List[Dict[str, Any]] = []
    for pitcher in pitcher_map.values():
        opp_team_id = pitcher.get("opp_team_id")
        # Determine which lineup faces this pitcher
        if opp_team_id == home_team_id:
            lineup = home_lineup
            offense_team = home_team
        elif opp_team_id == away_team_id:
            lineup = away_lineup
            offense_team = away_team
        else:
            lineup = []
            offense_team = None

        lineup_confirmed = len(lineup) >= 9
        weak_spot_count = 0

        for pos in pitcher["positions"]:
            bo = pos["batting_order"]
            if bo is not None and bo >= 1 and bo <= len(lineup):
                pid = lineup[bo - 1]
                pos["player_id"] = pid
                pos["player_name"] = name_map.get(pid)
            if pos["is_weak_spot"]:
                weak_spot_count += 1

        pitcher["offense_team"] = offense_team
        pitcher["lineup_confirmed"] = lineup_confirmed
        pitcher["weak_spot_count"] = weak_spot_count
        pitcher["positions"].sort(key=lambda p: p.get("batting_order") or 0)
        pitchers_out.append(pitcher)

    game_weather = {
        "weather_indicator": gw.get("weather_indicator"),
        "game_temp": _safe_float(gw.get("game_temp")),
        "wind_speed": _safe_float(gw.get("wind_speed")),
        "wind_dir": _safe_int(gw.get("wind_dir")),
        "wind_direction_label": _wind_direction_label(_safe_int(gw.get("wind_dir"))),
        "precip_prob": _safe_float(gw.get("precip_prob")),
        "ballpark_name": _clean_str(gw.get("ballpark_name")),
        "roof_type": _clean_str(gw.get("roof_type")),
        "weather_note": _clean_str(gw.get("weather_note")),
    }
    game_odds = {
        "home_moneyline": _safe_int(gw.get("home_moneyline")),
        "away_moneyline": _safe_int(gw.get("away_moneyline")),
        "over_under": _safe_float(gw.get("over_under")),
    }

    return {
        "game_pk": game_pk,
        "run_date": today,
        "game": {
            "home_team": home_team,
            "away_team": away_team,
            "start_time_utc": schedule.get("start_time_utc") if schedule else None,
            "venue_name": gw.get("ballpark_name") or (schedule.get("venue_name") if schedule else None),
            "home_pitcher_name": schedule.get("home_pitcher_name") if schedule else None,
            "away_pitcher_name": schedule.get("away_pitcher_name") if schedule else None,
            "weather": game_weather,
            "odds": game_odds,
        },
        "pitchers": pitchers_out,
    }
