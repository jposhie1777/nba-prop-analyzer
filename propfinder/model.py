"""
model.py - PropFinder HR Pulse Score
Scale: 0-100. Labels: IDEAL (75+) | FAVORABLE (55-74) | AVERAGE (35-54) | AVOID (<35)

Fixes v2:
- pitcher_hand normalized from LHP/RHP to L/R for hit_data filter
- pitch log deduped by pitch name (keep highest pct), 2025 only
- batter matched to their actual pitcher via batter_team_id = opp_team_id
- splits lookup uses normalized hand char
"""

import datetime
import json
import logging
import re
from collections import defaultdict
from statistics import mean
from zoneinfo import ZoneInfo
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen

from google.cloud import bigquery

PROJECT = "graphite-flare-477419-h7"
DATASET = "propfinder"
BASE_URL = "https://api.propfinder.app"
SLATE_TZ = ZoneInfo("America/New_York")
TODAY = datetime.datetime.now(SLATE_TZ).date()
NOW = datetime.datetime.now(datetime.timezone.utc)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
bq = bigquery.Client(project=PROJECT)


def tbl(name):
    return f"`{PROJECT}.{DATASET}.{name}`"


# Thresholds
P_HR9_IDEAL = 1.8
P_HR9_FAV = 1.5
P_HR9_AVG = 1.2
P_HR9_AVOID = 0.9
P_HRFB_IDEAL = 20.0
P_HRFB_FAV = 15.0
P_HRFB_AVG = 9.0
P_FB_IDEAL = 40.0
P_FB_FAV = 35.0
P_BARREL_ELITE = 10.0
P_HARDHIT_ATTACK = 40.0
P_ISO_EXPLOIT = 0.200

B_ISO_ELITE = 0.300
B_ISO_FAV = 0.200
B_ISO_AVG = 0.150
B_SLG_ELITE = 0.500
B_SLG_FAV = 0.450
B_SLG_AVG = 0.400
B_EV_ELITE = 92.0
B_EV_FAV = 89.0
B_EV_AVG = 85.0
B_BAR_ELITE = 20.0
B_BAR_FAV = 12.0
B_BAR_AVG = 7.0

PULSE_IDEAL = 72
PULSE_FAV = 52
PULSE_AVG = 33
RAW_MAX = 125.0


def query(sql):
    return [dict(row) for row in bq.query(sql).result()]


def safe_float(value, default=0.0):
    try:
        s = str(value or "0")
        return float("0" + s if s.startswith(".") else s)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except (ValueError, TypeError):
        return default


def clean_str(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_book(book_name):
    book = str(book_name or "").strip().lower().replace(" ", "")
    if not book:
        return ""
    if book in ("dk", "draftkings"):
        return "draftkings"
    if book in ("fd", "fanduel"):
        return "fanduel"
    return book


def parse_draftkings_link(url):
    text = clean_str(url)
    if not text:
        return None, None
    split = urlsplit(text)
    if "draftkings.com" not in split.netloc.lower():
        return None, None
    match = re.search(r"/event/([^/?#]+)", split.path)
    event_id = match.group(1) if match else None

    outcome_code = None
    for part in (split.query or "").split("&"):
        if part.startswith("outcomes="):
            # Keep raw query payload (including %23) for app-link compatibility.
            outcome_code = part.split("=", 1)[1].strip() or None
            break
    return event_id, outcome_code


def parse_fanduel_link(url):
    text = clean_str(url)
    if not text:
        return None, None
    split = urlsplit(text)
    if "fanduel.com" not in split.netloc.lower():
        return None, None
    query = parse_qs(split.query, keep_blank_values=False)
    market_id = (query.get("marketId") or query.get("marketId[]") or [None])[0]
    selection_id = (query.get("selectionId") or query.get("selectionId[]") or [None])[0]
    return clean_str(market_id), clean_str(selection_id)


def load_hr_prop_context():
    """
    Load today's 1+ HR over props and parse sportsbook deep links.
    """
    url = f"{BASE_URL}/mlb/props?date={TODAY.isoformat()}"
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
    except Exception as exc:
        log.warning("Failed to load /mlb/props context: %s", exc)
        return {}

    if not isinstance(payload, list):
        return {}

    context = {}
    for row in payload:
        if not isinstance(row, dict):
            continue

        category = str(row.get("category") or "").lower()
        category_key = category.replace("_", "")
        if "homerun" not in category_key:
            continue
        if str(row.get("overUnder") or "").lower() != "over":
            continue
        line = safe_float(row.get("line"), default=None)
        if line is not None and abs(line - 0.5) > 1e-6:
            continue

        game_pk = safe_int(row.get("gameId"), default=None)
        batter_id = safe_int(row.get("playerId"), default=None)
        if game_pk is None or batter_id is None:
            continue

        markets = row.get("markets") if isinstance(row.get("markets"), list) else []
        best_market = row.get("bestMarket") if isinstance(row.get("bestMarket"), dict) else None
        all_markets = [m for m in markets if isinstance(m, dict)]
        if best_market:
            all_markets.append(best_market)

        best_price = None
        best_book = None
        best_desktop = None
        best_ios = None

        dk_event_id = None
        dk_outcome_code = None
        fd_market_id = None
        fd_selection_id = None

        for market in all_markets:
            sportsbook = clean_str(market.get("sportsbook"))
            desktop_link = clean_str(market.get("deepLinkDesktop"))
            ios_link = (
                clean_str(market.get("deepLinkIos"))
                or clean_str(market.get("deepLinkIOS"))
                or clean_str(market.get("deepLinkAndroid"))
            )
            price = safe_int(market.get("price"), default=None)

            if price is not None and (best_price is None or price > best_price):
                best_price = price
                best_book = sportsbook
                best_desktop = desktop_link
                best_ios = ios_link

            book_key = normalize_book(sportsbook)
            if book_key == "draftkings" and desktop_link:
                event_id, outcome_code = parse_draftkings_link(desktop_link)
                dk_event_id = dk_event_id or event_id
                dk_outcome_code = dk_outcome_code or outcome_code
            if book_key == "fanduel" and desktop_link:
                market_id, selection_id = parse_fanduel_link(desktop_link)
                fd_market_id = fd_market_id or market_id
                fd_selection_id = fd_selection_id or selection_id

        key = (game_pk, batter_id)
        existing = context.get(key, {})

        def _pick(*values):
            for value in values:
                if isinstance(value, str):
                    text = value.strip()
                    if text:
                        return text
                elif value is not None:
                    return value
            return None

        context[key] = {
            "home_team": _pick(existing.get("home_team"), row.get("homeTeam"), row.get("homeTeamCode")),
            "away_team": _pick(existing.get("away_team"), row.get("awayTeam"), row.get("opposingTeamCode")),
            "weather_indicator": _pick(existing.get("weather_indicator"), row.get("weatherIndicator"), row.get("weather_indicator")),
            "game_temp": _pick(existing.get("game_temp"), safe_float(row.get("gameTemp"), default=None), safe_float(row.get("temperature"), default=None), safe_float(row.get("game_temp"), default=None)),
            "wind_speed": _pick(existing.get("wind_speed"), safe_float(row.get("windSpeed"), default=None), safe_float(row.get("wind_speed"), default=None)),
            "wind_dir": _pick(existing.get("wind_dir"), safe_int(row.get("windDir"), default=None), safe_int(row.get("windDirection"), default=None), safe_int(row.get("wind_dir"), default=None)),
            "precip_prob": _pick(existing.get("precip_prob"), safe_float(row.get("precipProb"), default=None), safe_float(row.get("precip_prob"), default=None)),
            "ballpark_name": _pick(existing.get("ballpark_name"), row.get("ballparkName"), row.get("stadium"), row.get("venueName")),
            "roof_type": _pick(existing.get("roof_type"), row.get("roofType"), row.get("roof_type")),
            "weather_note": _pick(existing.get("weather_note"), row.get("weatherNote"), row.get("weather_note")),
            "home_moneyline": _pick(existing.get("home_moneyline"), safe_int(row.get("homeMoneyline"), default=None), safe_int(row.get("home_moneyline"), default=None)),
            "away_moneyline": _pick(existing.get("away_moneyline"), safe_int(row.get("awayMoneyline"), default=None), safe_int(row.get("away_moneyline"), default=None)),
            "over_under": _pick(existing.get("over_under"), safe_float(row.get("gameTotal"), default=None), safe_float(row.get("over_under"), default=None)),
            "hr_odds_best_price": _pick(best_price, existing.get("hr_odds_best_price")),
            "hr_odds_best_book": _pick(best_book, existing.get("hr_odds_best_book")),
            "deep_link_desktop": _pick(best_desktop, existing.get("deep_link_desktop")),
            "deep_link_ios": _pick(best_ios, existing.get("deep_link_ios")),
            "dk_event_id": _pick(dk_event_id, existing.get("dk_event_id")),
            "dk_outcome_code": _pick(dk_outcome_code, existing.get("dk_outcome_code")),
            "fd_market_id": _pick(fd_market_id, existing.get("fd_market_id")),
            "fd_selection_id": _pick(fd_selection_id, existing.get("fd_selection_id")),
        }

    return context


def tier(value, elite, fav, avg):
    if value is None:
        return "below"
    if value >= elite:
        return "elite"
    if value >= fav:
        return "favorable"
    if value >= avg:
        return "average"
    return "below"


def hc(pitcher_hand):
    """Normalize LHP/RHP or L/R to single char."""
    return "L" if str(pitcher_hand or "").upper() in ("L", "LHP") else "R"


def load_hit_data():
    rows = query(
        f"""
        SELECT *
        FROM {tbl('raw_hit_data')}
        WHERE run_date = '{TODAY}'
        ORDER BY batter_id, event_date DESC
        """
    )
    out = defaultdict(list)
    for row in rows:
        out[row["batter_id"]].append(row)
    return out


def load_splits():
    rows = query(
        f"""
        SELECT *
        FROM {tbl('raw_splits')}
        WHERE run_date = '{TODAY}'
        """
    )
    out = defaultdict(dict)
    for row in rows:
        out[row["batter_id"]][row["split_code"]] = row
    return out


def load_pitcher_matchup():
    rows = query(
        f"""
        SELECT *
        FROM {tbl('raw_pitcher_matchup')}
        WHERE run_date = '{TODAY}'
        """
    )
    out = defaultdict(dict)
    for row in rows:
        out[row["pitcher_id"]][row["split"]] = row
    return out


def load_pitch_log():
    rows = query(
        f"""
        SELECT *
        FROM {tbl('raw_pitch_log')}
        WHERE run_date = '{TODAY}' AND season = 2025
        ORDER BY pitcher_id, percentage DESC
        """
    )
    out = defaultdict(list)
    for row in rows:
        out[row["pitcher_id"]].append(row)
    return out


def load_game_weather():
    """Load weather + odds data from raw_game_weather for today."""
    try:
        rows = query(
            f"""
            SELECT
                game_pk,
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
            FROM {tbl('raw_game_weather')}
            WHERE run_date = '{TODAY}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY ingested_at DESC) = 1
            """
        )
        return {row["game_pk"]: row for row in rows}
    except Exception as exc:
        log.warning("load_game_weather failed (table may not exist yet): %s", exc)
        return {}


def load_hr_props():
    """Load HR prop odds from raw_hr_props for today, keyed by (game_pk, player_id)."""
    try:
        rows = query(
            f"""
            SELECT
                game_pk,
                player_id,
                hr_odds_best_price,
                hr_odds_best_book,
                deep_link_desktop,
                deep_link_ios,
                dk_outcome_code,
                dk_event_id,
                fd_market_id,
                fd_selection_id
            FROM {tbl('raw_hr_props')}
            WHERE run_date = '{TODAY}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk, player_id ORDER BY ingested_at DESC) = 1
            """
        )
        return {(int(row["game_pk"]), int(row["player_id"])): row for row in rows}
    except Exception as exc:
        log.warning("load_hr_props failed (table may not exist yet): %s", exc)
        return {}


def load_today_matchups():
    """
    Join on batter_team_id = opp_team_id so each batter only faces
    the pitcher whose team they are NOT on.
    """
    return query(
        f"""
        SELECT DISTINCT
            hd.game_pk,
            hd.batter_id,
            hd.batter_name,
            hd.bat_side,
            hd.batter_team_id,
            pm.pitcher_id,
            pm.pitcher_name,
            pm.pitcher_hand,
            pm.opp_team_id
        FROM {tbl('raw_hit_data')} hd
        JOIN {tbl('raw_pitcher_matchup')} pm
          ON hd.game_pk = pm.game_pk
         AND pm.run_date = '{TODAY}'
         AND pm.split = 'Season'
         AND hd.batter_team_id = pm.opp_team_id
        WHERE hd.run_date = '{TODAY}'
        """
    )


def compute_batter_metrics(batter_id, bat_side, pitcher_hand_raw, hit_events, splits, pitch_log):
    hand_char = hc(pitcher_hand_raw)
    hand_code = "LHB" if bat_side == "L" else "RHB"

    # Dedupe pitch log: keep highest pct per pitch name, 2025 only.
    seen = {}
    for pitch in pitch_log:
        if pitch.get("batter_hand") != hand_code:
            continue
        name = pitch.get("pitch_name", "")
        pct = safe_float(pitch.get("percentage"))
        if name and (name not in seen or pct > seen[name]):
            seen[name] = pct
    primary = {name for name, pct in seen.items() if pct > 0.05}

    filtered = [
        event
        for event in hit_events
        if event.get("pitch_hand") == hand_char
        and (not primary or event.get("pitch_type") in primary)
    ]

    l15 = filtered[:15]
    n15 = len(l15)
    l15_barrel = (
        sum(1 for event in l15 if event.get("is_barrel")) / n15 * 100 if n15 else 0.0
    )
    l15_ev = mean(event["launch_speed"] for event in l15) if n15 else 0.0
    l15_hh = (
        sum(1 for event in l15 if safe_float(event.get("launch_speed")) >= 95) / n15 * 100
        if n15
        else 0.0
    )

    log.debug(
        "Batter %s: %s filtered events, %s in L15, pitches=%s",
        batter_id,
        len(filtered),
        n15,
        sorted(primary),
    )

    season_2025 = [event for event in hit_events if event.get("season") == 2025]
    n25 = len(season_2025)
    s25_barrel = (
        sum(1 for event in season_2025 if event.get("is_barrel")) / n25 * 100 if n25 else 0.0
    )
    s25_ev = mean(event["launch_speed"] for event in season_2025) if n25 else 0.0

    fly_balls = [event for event in hit_events if event.get("trajectory") == "fly_ball"]
    hr_fb = (
        sum(1 for event in fly_balls if event.get("result") == "home_run")
        / len(fly_balls)
        * 100
        if fly_balls
        else 0.0
    )

    split_key = "vl" if hand_char == "L" else "vr"
    split_row = splits.get(split_key, {})
    at_bats = int(split_row.get("at_bats") or 0)
    home_runs = int(split_row.get("home_runs") or 0)
    doubles = int(split_row.get("doubles") or 0)
    triples = int(split_row.get("triples") or 0)
    iso = (doubles + 2 * triples + 3 * home_runs) / at_bats if at_bats > 0 else 0.0
    slg = safe_float(split_row.get("slg"))

    return {
        "iso": round(iso, 3),
        "slg": round(slg, 3),
        "l15_ev": round(l15_ev, 1),
        "l15_barrel_pct": round(l15_barrel, 1),
        "season_ev": round(s25_ev, 1),
        "season_barrel_pct": round(s25_barrel, 1),
        "l15_hard_hit_pct": round(l15_hh, 1),
        "hr_fb_pct": round(hr_fb, 1),
    }


def compute_pitcher_metrics(pitcher_id, bat_side, matchup_splits):
    del pitcher_id  # currently unused, kept for interface stability

    hand_split = matchup_splits.get("vsLHB" if bat_side == "L" else "vsRHB", {})
    season_split = matchup_splits.get("Season", {})

    p_hr9 = safe_float(hand_split.get("hr_per_9"))
    p_hr9_season = safe_float(season_split.get("hr_per_9"))
    p_hr_n = int(hand_split.get("home_runs") or 0)
    p_ip = safe_float(hand_split.get("ip"))

    raw_barrel = hand_split.get("barrel_pct") or season_split.get("barrel_pct") or 0
    p_barrel = safe_float(raw_barrel)
    if p_barrel < 1:
        p_barrel *= 100

    raw_hh = season_split.get("hard_hit_pct") or 0
    p_hh = safe_float(raw_hh)
    if p_hh < 1:
        p_hh *= 100

    raw_fb = hand_split.get("fb_pct") or season_split.get("fb_pct") or 0
    p_fb = safe_float(raw_fb)
    if p_fb < 1:
        p_fb *= 100

    stored_hrfb = safe_float(hand_split.get("hr_fb_pct"))
    if stored_hrfb > 0:
        p_hrfb = stored_hrfb
    elif p_ip > 0 and p_fb > 0:
        est_fb = p_ip * (p_fb / 100) * 1.2
        p_hrfb = min((p_hr_n / est_fb) * 100, 60.0) if est_fb > 0 else 0.0
    else:
        p_hrfb = 0.0

    woba = safe_float(hand_split.get("woba"))
    p_iso = 0.230 if woba >= 0.340 else (0.175 if woba >= 0.310 else 0.125)

    return {
        "p_hr9_season": round(p_hr9_season, 2),
        "p_hr9_vs_hand": round(p_hr9, 2),
        "p_barrel_pct": round(p_barrel, 1),
        "p_hr_fb_pct": round(p_hrfb, 1),
        "p_hr_vs_hand": p_hr_n,
        "p_fb_pct": round(p_fb, 1),
        "p_hard_hit_pct": round(p_hh, 1),
        "p_iso_allowed": round(p_iso, 3),
    }


def compute_pulse_score(bm, pm, bat_side, pitcher_hand_raw):
    raw = 0.0
    flags_good = []
    flags_bad = []

    hand_char_val = hc(pitcher_hand_raw)
    hitter_hand = "LHB" if bat_side == "L" else "RHB"

    hr9 = pm["p_hr9_vs_hand"]
    hrfb = pm["p_hr_fb_pct"]

    # Pitcher (max 65 points).
    if hr9 >= P_HR9_IDEAL:
        raw += 20
        flags_good.append(f"HR/9 vs {hitter_hand}: {hr9:.2f} (elite target)")
    elif hr9 >= P_HR9_FAV:
        raw += 15
        flags_good.append(f"HR/9 vs {hitter_hand}: {hr9:.2f} (favorable)")
    elif hr9 >= P_HR9_AVG:
        raw += 8
        flags_good.append(f"HR/9 vs {hitter_hand}: {hr9:.2f} (average)")
    elif hr9 < P_HR9_AVOID:
        raw -= 3
        flags_bad.append(f"HR/9 vs {hitter_hand}: {hr9:.2f} (avoid)")

    if hrfb >= P_HRFB_IDEAL:
        raw += 18
        flags_good.append(f"HR/FB% vs {hitter_hand}: {hrfb:.1f}% (elite target)")
    elif hrfb >= P_HRFB_FAV:
        raw += 12
        flags_good.append(f"HR/FB% vs {hitter_hand}: {hrfb:.1f}% (favorable)")
    elif hrfb >= P_HRFB_AVG:
        raw += 5
        flags_good.append(f"HR/FB% vs {hitter_hand}: {hrfb:.1f}% (average)")
    else:
        raw -= 1
        flags_bad.append(f"HR/FB% vs {hitter_hand}: {hrfb:.1f}% (avoid)")

    fb = pm["p_fb_pct"]
    if fb > P_FB_IDEAL:
        raw += 8
        flags_good.append(f"FB%: {fb:.1f}%")
    elif fb >= P_FB_FAV:
        raw += 4
        flags_good.append(f"FB%: {fb:.1f}%")

    if pm["p_barrel_pct"] > P_BARREL_ELITE:
        raw += 7
        flags_good.append(f"Barrel% allowed: {pm['p_barrel_pct']:.1f}%")
    elif pm["p_barrel_pct"] >= 7:
        raw += 3
        flags_good.append(f"Barrel% allowed: {pm['p_barrel_pct']:.1f}%")

    if pm["p_hard_hit_pct"] >= P_HARDHIT_ATTACK:
        raw += 5
        flags_good.append(f"HardHit% allowed: {pm['p_hard_hit_pct']:.1f}%")
    elif pm["p_hard_hit_pct"] >= 35:
        raw += 2

    if pm["p_iso_allowed"] >= P_ISO_EXPLOIT:
        raw += 4
        flags_good.append(f"ISO allowed: {pm['p_iso_allowed']:.3f}")
    elif pm["p_iso_allowed"] >= 0.160:
        raw += 2

    if bat_side == hand_char_val:
        raw += 3
        flags_good.append(f"{hitter_hand} platoon edge")

    # Batter (max 60 points).
    iso_tier = tier(bm["iso"], B_ISO_ELITE, B_ISO_FAV, B_ISO_AVG)
    slg_tier = tier(bm["slg"], B_SLG_ELITE, B_SLG_FAV, B_SLG_AVG)
    ev_tier = tier(bm["l15_ev"], B_EV_ELITE, B_EV_FAV, B_EV_AVG)
    barrel_tier = tier(bm["l15_barrel_pct"], B_BAR_ELITE, B_BAR_FAV, B_BAR_AVG)

    points = {"elite": 15, "favorable": 10, "average": 5, "below": 0}
    raw += points[iso_tier] + points[slg_tier] + points[ev_tier] + points[barrel_tier]

    if iso_tier == "below":
        raw -= 1
        flags_bad.append(f"ISO {bm['iso']:.3f} (weak)")
    else:
        flags_good.append(f"ISO {bm['iso']:.3f} ({iso_tier})")
    if slg_tier != "below":
        flags_good.append(f"SLG {bm['slg']:.3f} ({slg_tier})")
    if ev_tier != "below":
        flags_good.append(f"L15 EV {bm['l15_ev']} mph ({ev_tier})")
    if barrel_tier != "below":
        flags_good.append(f"L15 Barrel {bm['l15_barrel_pct']:.1f}% ({barrel_tier})")

    if bm["season_barrel_pct"] >= B_BAR_ELITE:
        flags_good.append(f"2025 Barrel {bm['season_barrel_pct']:.1f}% (elite)")
    elif bm["season_barrel_pct"] >= B_BAR_FAV:
        flags_good.append(f"2025 Barrel {bm['season_barrel_pct']:.1f}% (favorable)")

    # Keep elite recent form from being overly suppressed by strong pitcher baselines.
    hot_form_bonus = 0
    if bm["l15_ev"] >= B_EV_ELITE:
        hot_form_bonus += 4
    if bm["l15_barrel_pct"] >= B_BAR_ELITE:
        hot_form_bonus += 4
    if bm["l15_hard_hit_pct"] >= 50:
        hot_form_bonus += 3
    if hot_form_bonus > 0:
        raw += hot_form_bonus
        flags_good.append(f"Hot-form bonus +{hot_form_bonus}")

    pulse = round(max(0.0, min(100.0, (raw / RAW_MAX) * 100)), 1)

    # Keep the avoid guardrail, but don't force AVOID on clear elite recent hitter form.
    pitcher_avoid = hr9 < P_HR9_AVOID and hrfb < (P_HRFB_AVG - 1.0)
    hitter_on_fire = bm["l15_ev"] >= B_EV_ELITE and bm["l15_barrel_pct"] >= B_BAR_ELITE
    if (pitcher_avoid and not hitter_on_fire) or pulse < PULSE_AVG:
        label = "AVOID"
    elif pulse >= PULSE_IDEAL:
        label = "IDEAL"
    elif pulse >= PULSE_FAV:
        label = "FAVORABLE"
    else:
        label = "AVERAGE"

    return pulse, label, flags_good, flags_bad


def build_why(batter_name, bat_side, pitcher_name, pitcher_hand_raw, bm, pm, label, flags_good, flags_bad):
    del batter_name  # Not currently used in assembled sentence.

    hitter_hand = "LHB" if bat_side == "L" else "RHB"
    pitcher_hand = "LHP" if hc(pitcher_hand_raw) == "L" else "RHP"

    parts = []
    if label == "IDEAL":
        if pm["p_hr9_vs_hand"] >= P_HR9_FAV:
            parts.append(
                f"{pitcher_hand} {pitcher_name} is exploitable vs {hitter_hand}s "
                f"(HR/9: {pm['p_hr9_vs_hand']:.2f})"
            )
        if pm["p_hr_fb_pct"] >= P_HRFB_FAV:
            parts.append(f"HR/FB% of {pm['p_hr_fb_pct']:.1f}% indicates power risk")
        if bm["l15_barrel_pct"] >= B_BAR_ELITE:
            parts.append(f"Elite L15 Barrel% ({bm['l15_barrel_pct']:.1f}%)")
        if bm["iso"] >= B_ISO_ELITE:
            parts.append(f"Elite ISO {bm['iso']:.3f} vs {pitcher_hand}s")
    elif label == "FAVORABLE":
        if pm["p_hr9_vs_hand"] >= P_HR9_AVG:
            parts.append(
                f"{pitcher_name} ({pitcher_hand}) HR/9 vs {hitter_hand}: "
                f"{pm['p_hr9_vs_hand']:.2f}"
            )
        if bm["l15_barrel_pct"] >= B_BAR_FAV:
            parts.append(f"Strong Barrel% trend ({bm['l15_barrel_pct']:.1f}%)")
        if bm["iso"] >= B_ISO_FAV:
            parts.append(f"Favorable ISO {bm['iso']:.3f}")
        if bm["l15_ev"] >= B_EV_FAV:
            parts.append(f"L15 EV {bm['l15_ev']:.1f} mph")
    elif label == "AVERAGE":
        parts.append("Some positive signals are present but not fully confirmed")
        if bm["l15_ev"] >= B_EV_ELITE:
            parts.append(f"Elite contact quality (L15 EV {bm['l15_ev']:.1f})")
        parts.append("Use sparingly")
    else:
        parts.append("Pitcher is not exploitable against this handedness")

    if flags_bad:
        cleaned = ", ".join(flag.replace(" (avoid)", "") for flag in flags_bad)
        parts.append(f"Issues: {cleaned}")
    return ". ".join(parts) + "." if parts else ""


def main():
    log.info("Starting PropFinder Pulse Score model for %s", TODAY)

    hit_data_map = load_hit_data()
    splits_map = load_splits()
    pitcher_map = load_pitcher_matchup()
    pitch_log_map = load_pitch_log()
    weather_map = load_game_weather()
    props_map = load_hr_props()
    matchups = load_today_matchups()
    hr_prop_context = load_hr_prop_context()
    log.info("Loaded HR prop context for %s batter/game rows", len(hr_prop_context))
    log.info("Loaded game weather rows: %s | raw HR props rows: %s", len(weather_map), len(props_map))

    if not matchups:
        log.warning("No matchup data - did ingest run with the updated ingest.py?")
        return

    log.info("Processing %s batter/pitcher matchups", len(matchups))

    output_rows = []
    seen = set()

    for matchup in matchups:
        key = (matchup["batter_id"], matchup["pitcher_id"], matchup["game_pk"])
        if key in seen:
            continue
        seen.add(key)

        hit_events = hit_data_map.get(matchup["batter_id"], [])
        if not hit_events:
            continue

        batter_metrics = compute_batter_metrics(
            matchup["batter_id"],
            matchup["bat_side"],
            matchup["pitcher_hand"],
            hit_events,
            splits_map.get(matchup["batter_id"], {}),
            pitch_log_map.get(matchup["pitcher_id"], []),
        )
        pitcher_metrics = compute_pitcher_metrics(
            matchup["pitcher_id"],
            matchup["bat_side"],
            pitcher_map.get(matchup["pitcher_id"], {}),
        )
        score, grade, flags_good, flags_bad = compute_pulse_score(
            batter_metrics,
            pitcher_metrics,
            matchup["bat_side"],
            matchup["pitcher_hand"],
        )
        why = build_why(
            matchup["batter_name"],
            matchup["bat_side"],
            matchup["pitcher_name"],
            matchup["pitcher_hand"],
            batter_metrics,
            pitcher_metrics,
            grade,
            flags_good,
            flags_bad,
        )
        prop_ctx = hr_prop_context.get((matchup["game_pk"], matchup["batter_id"]), {})

        gw = weather_map.get(matchup["game_pk"], {})
        pr = props_map.get((matchup["game_pk"], matchup["batter_id"]), {})
        output_rows.append(
            {
                "run_date": TODAY.isoformat(),
                "run_timestamp": NOW.isoformat(),
                "game_pk": matchup["game_pk"],
                "home_team": prop_ctx.get("home_team") or gw.get("home_team_name") or "",
                "away_team": prop_ctx.get("away_team") or gw.get("away_team_name") or "",
                "batter_id": matchup["batter_id"],
                "batter_name": matchup["batter_name"],
                "bat_side": matchup["bat_side"],
                "pitcher_id": matchup["pitcher_id"],
                "pitcher_name": matchup["pitcher_name"],
                "pitcher_hand": matchup["pitcher_hand"],
                **batter_metrics,
                **pitcher_metrics,
                "score": score,
                "grade": grade,
                "why": why,
                "flags": json.dumps(flags_good + flags_bad),
                "weather_indicator": prop_ctx.get("weather_indicator") or gw.get("weather_indicator"),
                "game_temp": prop_ctx.get("game_temp") if prop_ctx.get("game_temp") is not None else gw.get("game_temp"),
                "wind_speed": prop_ctx.get("wind_speed") if prop_ctx.get("wind_speed") is not None else gw.get("wind_speed"),
                "wind_dir": prop_ctx.get("wind_dir") if prop_ctx.get("wind_dir") is not None else gw.get("wind_dir"),
                "precip_prob": prop_ctx.get("precip_prob") if prop_ctx.get("precip_prob") is not None else gw.get("precip_prob"),
                "ballpark_name": prop_ctx.get("ballpark_name") or gw.get("ballpark_name"),
                "roof_type": prop_ctx.get("roof_type") or gw.get("roof_type"),
                "weather_note": prop_ctx.get("weather_note") or gw.get("weather_note"),
                "home_moneyline": prop_ctx.get("home_moneyline") if prop_ctx.get("home_moneyline") is not None else gw.get("home_moneyline"),
                "away_moneyline": prop_ctx.get("away_moneyline") if prop_ctx.get("away_moneyline") is not None else gw.get("away_moneyline"),
                "over_under": prop_ctx.get("over_under") if prop_ctx.get("over_under") is not None else gw.get("over_under"),
                "hr_odds_best_price": prop_ctx.get("hr_odds_best_price") if prop_ctx.get("hr_odds_best_price") is not None else pr.get("hr_odds_best_price"),
                "hr_odds_best_book": prop_ctx.get("hr_odds_best_book") or pr.get("hr_odds_best_book"),
                "deep_link_desktop": prop_ctx.get("deep_link_desktop") or pr.get("deep_link_desktop"),
                "deep_link_ios": prop_ctx.get("deep_link_ios") or pr.get("deep_link_ios"),
                "dk_outcome_code": prop_ctx.get("dk_outcome_code") or pr.get("dk_outcome_code"),
                "dk_event_id": prop_ctx.get("dk_event_id") or pr.get("dk_event_id"),
                "fd_market_id": prop_ctx.get("fd_market_id") or pr.get("fd_market_id"),
                "fd_selection_id": prop_ctx.get("fd_selection_id") or pr.get("fd_selection_id"),
            }
        )

    output_rows.sort(key=lambda row: row["score"], reverse=True)
    log.info("Scored %s matchups", len(output_rows))

    if output_rows:
        errors = bq.insert_rows_json(f"{PROJECT}.{DATASET}.hr_picks_daily", output_rows)
        if errors:
            log.error("BQ insert errors: %s", errors[:3])
        else:
            log.info("Wrote %s picks to hr_picks_daily", len(output_rows))

    print(f"\n{'=' * 70}")
    print(f"PULSE SCORE - HR PICKS - {TODAY}")
    print(f"{'=' * 70}")
    for label in ["IDEAL", "FAVORABLE", "AVERAGE", "AVOID"]:
        group = [row for row in output_rows if row["grade"] == label]
        if not group:
            continue
        print(f"\n-- {label} --")
        for row in group:
            hitter_hand = "LHB" if row["bat_side"] == "L" else "RHB"
            pitcher_hand = "LHP" if hc(row["pitcher_hand"]) == "L" else "RHP"
            print(
                f"  {row['batter_name']:<22} ({hitter_hand}) vs "
                f"{row['pitcher_name']:<18} ({pitcher_hand})  Pulse: {row['score']}"
            )
            print(
                f"    ISO:{row['iso']:.3f}  SLG:{row['slg']:.3f}  "
                f"L15EV:{row['l15_ev']}  L15Barrel:{row['l15_barrel_pct']:.1f}%  "
                f"| P HR/9:{row['p_hr9_vs_hand']:.2f}  HR/FB:{row['p_hr_fb_pct']:.1f}%"
            )
            if row["why"]:
                print(f"    {row['why']}")

    skipped = sum(1 for row in output_rows if row["grade"] == "AVOID")
    print(f"\n-- AVOID: {skipped} matchups did not meet criteria --")


if __name__ == "__main__":
    main()
