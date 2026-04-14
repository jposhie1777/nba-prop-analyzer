# propfinder/hit_model.py

"""
hit_model.py — Hit Pulse Score: batter hits prop scoring system.

Scores OVER 0.5 hits (1+ hit) props on a 0-100 scale.
Self-learning: loads calibrated weights from hit_model_weights table
and falls back to baseline weights when no learned data exists.

Grades:
  FIRE   (80+)  — strongest signal
  STRONG (65-79) — high confidence
  LEAN   (50-64) — moderate edge
  SKIP   (<50)   — no actionable edge

Factors scored:
  1. Batter contact quality (AVG vs hand, contact rate, L15 hit rate, hard-hit%)
  2. Pitcher vulnerability (WHIP, K rate inverse, wOBA allowed, hard-hit allowed)
  3. Matchup (BvP history, platoon edge, pitcher arsenal contact rate)
  4. PropFinder consensus signals (pf_rating, hit rates, streak)
  5. Recent form vs line (avg_l10 vs line, avg_vs_opponent)
  6. Game environment (Vegas total, ballpark)
"""

import datetime
import json
import logging
from collections import defaultdict
from statistics import mean
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from google.cloud import bigquery

PROJECT = "graphite-flare-477419-h7"
DATASET = "propfinder"
SLATE_TZ = ZoneInfo("America/New_York")
TODAY = datetime.datetime.now(SLATE_TZ).date()
NOW = datetime.datetime.now(datetime.timezone.utc)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
bq = bigquery.Client(project=PROJECT)


def tbl(name):
    return f"`{PROJECT}.{DATASET}.{name}`"


def query(sql):
    return [dict(row) for row in bq.query(sql).result()]


def sf(val, default=0.0):
    try:
        s = str(val or "0")
        return float("0" + s if s.startswith(".") else s)
    except (ValueError, TypeError):
        return default


def si(val, default=0):
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


# ── Baseline weights (before learning kicks in) ──────────────────────────

BASELINE_WEIGHTS = {
    # Batter contact quality (40%)
    "batting_avg":           12.0,
    "contact_rate":          10.0,
    "l15_hit_rate":           8.0,
    "hard_hit_pct":           5.0,
    "babip_proxy":            5.0,
    # Pitcher vulnerability (25%)
    "p_whip":                10.0,
    "p_k_rate_inv":           8.0,
    "p_woba_allowed":         7.0,
    # Matchup (15%)
    "bvp_history":            7.0,
    "platoon_edge":           5.0,
    "arsenal_contact":        3.0,
    # PropFinder consensus (12%)
    "pf_rating":              5.0,
    "hit_rate_l10":           4.0,
    "hit_rate_season":        2.0,
    "hit_rate_vs_team":       2.0,
    # Weak spot (pitcher vulnerable at lineup position)
    "weak_spot":              6.0,
    # Context (8%)
    "vegas_total":            3.0,
    "avg_l10_vs_line":        3.0,
    "streak":                 2.0,
}

# Batter contact thresholds
AVG_ELITE = 0.310
AVG_STRONG = 0.275
AVG_AVG = 0.250
AVG_WEAK = 0.220

CONTACT_ELITE = 85.0    # 100 - K%
CONTACT_STRONG = 80.0
CONTACT_AVG = 75.0

# Pitcher vulnerability thresholds (inverse of K model)
WHIP_VULNERABLE = 1.40
WHIP_LEAKY = 1.25
WHIP_AVG = 1.15
WHIP_TOUGH = 1.00

KRATE_LOW = 18.0       # Low K% = batter-friendly
KRATE_BELOW_AVG = 22.0
KRATE_AVG = 25.0

WOBA_VULNERABLE = 0.340
WOBA_HITTABLE = 0.310
WOBA_AVG = 0.290

# Grade thresholds (relaxed for early season — tighten once calibration has data)
GRADE_FIRE = 68
GRADE_STRONG = 52
GRADE_LEAN = 38


# ── Data loaders ─────────────────────────────────────────────────────────

def load_hit_props():
    """Load today's hit props (OVER 0.5 only) from raw_hit_props."""
    rows = query(f"""
        SELECT *
        FROM {tbl('raw_hit_props')}
        WHERE run_date = '{TODAY}'
          AND over_under = 'over'
          AND line = 0.5
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY batter_id
            ORDER BY ingested_at DESC
        ) = 1
    """)
    return {r["batter_id"]: r for r in rows}


def load_hit_data():
    """Load raw_hit_data for computing contact metrics."""
    rows = query(f"""
        SELECT *
        FROM {tbl('raw_hit_data')}
        WHERE run_date = '{TODAY}'
        ORDER BY batter_id, event_date DESC
    """)
    out = defaultdict(list)
    for row in rows:
        out[row["batter_id"]].append(row)
    return out


def load_splits():
    """Load batter splits (vs LHP/RHP) for batting average."""
    rows = query(f"""
        SELECT *
        FROM {tbl('raw_splits')}
        WHERE run_date = '{TODAY}'
    """)
    out = defaultdict(dict)
    for row in rows:
        out[row["batter_id"]][row["split_code"]] = row
    return out


def load_pitcher_matchups():
    """Load pitcher matchup data with contact-relevant stats."""
    rows = query(f"""
        SELECT *
        FROM {tbl('raw_pitcher_matchup')}
        WHERE run_date = '{TODAY}'
    """)
    out = defaultdict(dict)
    for r in rows:
        out[r["pitcher_id"]][r["split"]] = r
    return out


def load_pitch_log():
    """Load pitch arsenal — whiff rates per pitch (low whiff = more contact)."""
    rows = query(f"""
        SELECT pitcher_id, batter_hand, pitch_name, percentage, whiff, k_percent, woba, slg
        FROM {tbl('raw_pitch_log')}
        WHERE run_date = '{TODAY}' AND season = {TODAY.year}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pitcher_id, batter_hand, pitch_name
            ORDER BY percentage DESC
        ) = 1
    """)
    out = defaultdict(list)
    for r in rows:
        out[r["pitcher_id"]].append(r)
    return out


def load_game_weather():
    """Load game context (Vegas totals, ballpark)."""
    rows = query(f"""
        SELECT game_pk, game_date, over_under, ballpark_name
        FROM {tbl('raw_game_weather')}
        WHERE run_date = '{TODAY}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY ingested_at DESC) = 1
    """)
    return {r["game_pk"]: r for r in rows}


def load_today_matchups():
    """Join hit_data with pitcher_matchup to get batter→pitcher assignments."""
    return query(f"""
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
    """)


def load_batting_positions():
    """Fetch batting order positions from PropFinder upcoming-games."""
    url = f"https://api.propfinder.app/mlb/upcoming-games?date={TODAY.isoformat()}"
    request = Request(url, headers={"User-Agent": "PulseSports/1.0", "Accept": "application/json"})
    positions = {}  # (game_pk, batter_id) → 1-9
    try:
        with urlopen(request, timeout=20) as response:
            data = json.loads(response.read().decode("utf-8"))
        for item in (data if isinstance(data, list) else []):
            game_pk = si(item.get("id"))
            if not game_pk:
                continue
            for side in ("homeBattingOrder", "visitorBattingOrder"):
                order_str = item.get(side, "")
                ids = [si(x) for x in str(order_str).split(",") if x.strip().isdigit()]
                for pos_idx, bid in enumerate(ids):
                    if bid:
                        positions[(game_pk, bid)] = pos_idx + 1
        log.info("Loaded batting positions for %s batter-game combos", len(positions))
    except Exception as exc:
        log.warning("Failed to load batting positions: %s", exc)
    return positions


def load_pitcher_vs_batting_order():
    """Load pitcher stats vs each batting order position."""
    try:
        rows = query(f"""
            SELECT pitcher_id, batting_order, at_bats, hits, home_runs, avg, slg, ops
            FROM {tbl('raw_pitcher_vs_batting_order')}
            WHERE run_date = '{TODAY}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY pitcher_id, batting_order ORDER BY ingested_at DESC
            ) = 1
        """)
        return {(row["pitcher_id"], row["batting_order"]): row for row in rows}
    except Exception as exc:
        log.warning("load_pitcher_vs_batting_order failed: %s", exc)
        return {}


def check_hit_weak_spot(pitcher_id, batting_pos, pvbo_map):
    """Return weak spot dict if pitcher gives up hits at this lineup position.
    Criteria: AVG >= .300 or 3+ hits (with >= 5 AB sample)."""
    if not batting_pos:
        return None
    row = pvbo_map.get((pitcher_id, batting_pos))
    if not row or (row.get("at_bats") or 0) < 5:
        return None
    avg = row.get("avg") or 0
    hits = row.get("hits") or 0
    if avg >= 0.300 or hits >= 3:
        return {
            "ws_batting_order": batting_pos,
            "ws_at_bats": row.get("at_bats"),
            "ws_hits": hits,
            "ws_avg": avg,
        }
    return None


def load_learned_weights():
    """Load the most recent learned weights from hit_model_weights."""
    try:
        rows = query(f"""
            SELECT factor, weight
            FROM {tbl('hit_model_weights')}
            WHERE run_date = (
                SELECT MAX(run_date) FROM {tbl('hit_model_weights')}
            )
        """)
        if rows:
            weights = {r["factor"]: r["weight"] for r in rows}
            log.info("Loaded %s learned hit weights", len(weights))
            return weights
    except Exception as exc:
        log.warning("Could not load learned hit weights: %s", exc)
    return None


def parse_hit_rate(rate_str):
    """Parse '7/10' → 0.7 or None."""
    if not rate_str or "/" not in str(rate_str):
        return None
    try:
        num, den = str(rate_str).split("/")
        return int(num) / int(den) if int(den) > 0 else None
    except (ValueError, ZeroDivisionError):
        return None


def fetch_bvp_stats(batter_id, pitcher_id):
    """Fetch career batter-vs-pitcher stats from MLB Stats API."""
    try:
        url = (
            f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats"
            f"?stats=vsPlayer&opposingPlayerId={pitcher_id}&group=hitting"
        )
        req = Request(url, headers={"User-Agent": "PulseSports/1.0"})
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for stat_group in data.get("stats", []):
            for split in stat_group.get("splits", []):
                s = split.get("stat", {})
                ab = si(s.get("atBats"))
                hits = si(s.get("hits"))
                if ab > 0:
                    return {"bvp_ab": ab, "bvp_hits": hits, "bvp_avg": round(hits / ab, 3)}
    except Exception as exc:
        log.debug("BvP fetch failed for %s vs %s: %s", batter_id, pitcher_id, exc)
    return {"bvp_ab": None, "bvp_hits": None, "bvp_avg": None}


def load_bvp_bulk(matchups):
    """Fetch BvP stats for all matchups."""
    import time as _time
    bvp_map = {}
    seen = set()
    for m in matchups:
        key = (m["batter_id"], m["pitcher_id"])
        if key in seen:
            continue
        seen.add(key)
    log.info("Fetching BvP stats for %s unique hit matchups", len(seen))
    for i, (batter_id, pitcher_id) in enumerate(seen):
        bvp_map[(batter_id, pitcher_id)] = fetch_bvp_stats(batter_id, pitcher_id)
        if (i + 1) % 20 == 0:
            _time.sleep(0.5)
    return bvp_map


# ── Metric computation ───────────────────────────────────────────────────

def compute_batter_contact_metrics(batter_id, bat_side, pitcher_hand, hit_events, splits):
    """Compute contact-focused batter metrics."""
    hand_char = "L" if str(pitcher_hand or "").upper() in ("L", "LHP") else "R"
    split_key = "vl" if hand_char == "L" else "vr"
    split_row = splits.get(split_key, {})

    # Batting avg vs hand
    at_bats = si(split_row.get("at_bats"))
    hits = si(split_row.get("hits"))
    avg_vs_hand = round(hits / at_bats, 3) if at_bats > 0 else 0.0

    # Contact rate = 1 - K%
    strike_outs = si(split_row.get("strike_outs"))
    contact_rate = round((1 - strike_outs / at_bats) * 100, 1) if at_bats > 0 else 75.0

    # L15 hit rate from raw_hit_data
    hand_code = "LHB" if bat_side == "L" else "RHB"
    filtered = [e for e in hit_events if e.get("pitch_hand") == hand_char]
    l15 = filtered[:15]
    n15 = len(l15)
    l15_hits = sum(1 for e in l15 if e.get("result") in ("single", "double", "triple", "home_run"))
    l15_hit_rate = round(l15_hits / n15, 3) if n15 > 0 else 0.0
    l15_avg = round(l15_hits / n15, 3) if n15 > 0 else 0.0

    # Hard-hit %
    l15_hh = round(
        sum(1 for e in l15 if sf(e.get("launch_speed")) >= 95) / n15 * 100, 1
    ) if n15 > 0 else 0.0

    # Ground ball % and line drive %
    batted = [e for e in l15 if e.get("trajectory")]
    n_batted = len(batted)
    gb_pct = round(
        sum(1 for e in batted if e.get("trajectory") == "ground_ball") / n_batted * 100, 1
    ) if n_batted > 0 else 0.0
    ld_pct = round(
        sum(1 for e in batted if e.get("trajectory") == "line_drive") / n_batted * 100, 1
    ) if n_batted > 0 else 0.0

    # BABIP proxy: (line drives + ground balls that are hits) / (total in play)
    in_play = [e for e in hit_events[:30] if e.get("trajectory") and e.get("result") != "strikeout"]
    ip_hits = [e for e in in_play if e.get("result") in ("single", "double", "triple")
               and e.get("trajectory") in ("line_drive", "ground_ball")]
    babip = round(len(ip_hits) / len(in_play), 3) if in_play else 0.0

    return {
        "batting_avg_vs_hand": avg_vs_hand,
        "contact_rate": contact_rate,
        "l15_hit_rate": l15_hit_rate,
        "l15_avg": l15_avg,
        "hard_hit_pct": l15_hh,
        "ground_ball_pct": gb_pct,
        "line_drive_pct": ld_pct,
        "babip_proxy": babip,
    }


def compute_pitcher_vulnerability(pitcher_id, bat_side, pitcher_splits, pitch_log):
    """Compute how hittable the pitcher is."""
    hand_split_key = "vsLHB" if bat_side == "L" else "vsRHB"
    hand_split = pitcher_splits.get(hand_split_key, {})
    season = pitcher_splits.get("Season", {})

    whip = sf(hand_split.get("whip") or season.get("whip"))

    # K rate (lower = more hittable)
    k_pct_raw = sf(hand_split.get("k_pct") or season.get("k_pct"))
    k_rate = k_pct_raw if k_pct_raw > 1 else k_pct_raw * 100

    # wOBA allowed
    woba = sf(hand_split.get("woba") or season.get("woba"))

    # Hard-hit allowed
    hh_raw = sf(season.get("hard_hit_pct"))
    hh = hh_raw if hh_raw > 1 else hh_raw * 100

    # Hits per 9 (approximate from WHIP - walks component)
    ip = sf(season.get("ip"))
    hits_per_9 = round(whip * 9 * 0.7, 2) if whip > 0 else 0.0  # ~70% of baserunners are hits

    # Arsenal contact rate: average (1 - whiff%) across pitches vs this hand
    hand_code = "LHB" if bat_side == "L" else "RHB"
    pitches = [p for p in pitch_log if p.get("batter_hand") == hand_code and sf(p.get("percentage")) > 0.05]
    if pitches:
        avg_contact = round(mean(100 - sf(p.get("whiff")) for p in pitches), 1)
    else:
        avg_contact = 75.0  # default

    return {
        "p_whip": round(whip, 2),
        "p_k_rate": round(k_rate, 1),
        "p_woba_allowed": round(woba, 3),
        "p_hard_hit_allowed": round(hh, 1),
        "p_hits_per_9": hits_per_9,
        "arsenal_contact_rate": avg_contact,
    }


# ── Scoring engine ───────────────────────────────────────────────────────

def score_hit_prop(batter_metrics, pitcher_metrics, bvp, prop, game_ctx, weights, weak_spot=None):
    """Score a batter's 1+ hit prop. Returns (score, grade, why, flags, combined_metrics)."""
    w = weights
    raw = 0.0
    flags_good = []
    flags_bad = []

    bm = batter_metrics
    pm = pitcher_metrics

    # ── 1. Batter contact quality ────────────────────────────────────────
    avg = bm["batting_avg_vs_hand"]
    if avg >= AVG_ELITE:
        raw += w["batting_avg"]
        flags_good.append(f"AVG vs hand: {avg:.3f} (elite)")
    elif avg >= AVG_STRONG:
        raw += w["batting_avg"] * 0.7
        flags_good.append(f"AVG vs hand: {avg:.3f} (strong)")
    elif avg >= AVG_AVG:
        raw += w["batting_avg"] * 0.35
    elif avg < AVG_WEAK:
        raw -= w["batting_avg"] * 0.3
        flags_bad.append(f"AVG vs hand: {avg:.3f} (weak)")

    cr = bm["contact_rate"]
    if cr >= CONTACT_ELITE:
        raw += w["contact_rate"]
        flags_good.append(f"Contact: {cr:.0f}% (elite)")
    elif cr >= CONTACT_STRONG:
        raw += w["contact_rate"] * 0.6
        flags_good.append(f"Contact: {cr:.0f}%")
    elif cr >= CONTACT_AVG:
        raw += w["contact_rate"] * 0.25
    else:
        raw -= w["contact_rate"] * 0.2
        flags_bad.append(f"Contact: {cr:.0f}% (K-prone)")

    l15hr = bm["l15_hit_rate"]
    if l15hr >= 0.400:
        raw += w["l15_hit_rate"]
        flags_good.append(f"L15 hits: {l15hr:.3f} (hot)")
    elif l15hr >= 0.300:
        raw += w["l15_hit_rate"] * 0.6
        flags_good.append(f"L15 hits: {l15hr:.3f}")
    elif l15hr >= 0.200:
        raw += w["l15_hit_rate"] * 0.2
    else:
        raw -= w["l15_hit_rate"] * 0.2
        flags_bad.append(f"L15 hits: {l15hr:.3f} (cold)")

    hh = bm["hard_hit_pct"]
    if hh >= 50:
        raw += w["hard_hit_pct"]
        flags_good.append(f"Hard-hit: {hh:.0f}%")
    elif hh >= 35:
        raw += w["hard_hit_pct"] * 0.5

    babip = bm["babip_proxy"]
    if babip >= 0.350:
        raw += w["babip_proxy"]
        flags_good.append(f"BABIP proxy: {babip:.3f}")
    elif babip >= 0.280:
        raw += w["babip_proxy"] * 0.4

    # ── 2. Pitcher vulnerability ─────────────────────────────────────────
    whip = pm["p_whip"]
    if whip >= WHIP_VULNERABLE:
        raw += w["p_whip"]
        flags_good.append(f"WHIP: {whip:.2f} (vulnerable)")
    elif whip >= WHIP_LEAKY:
        raw += w["p_whip"] * 0.65
        flags_good.append(f"WHIP: {whip:.2f} (leaky)")
    elif whip >= WHIP_AVG:
        raw += w["p_whip"] * 0.25
    elif whip <= WHIP_TOUGH:
        raw -= w["p_whip"] * 0.3
        flags_bad.append(f"WHIP: {whip:.2f} (tough)")

    k_rate = pm["p_k_rate"]
    if k_rate <= KRATE_LOW:
        raw += w["p_k_rate_inv"]
        flags_good.append(f"P K%: {k_rate:.0f}% (low — hittable)")
    elif k_rate <= KRATE_BELOW_AVG:
        raw += w["p_k_rate_inv"] * 0.5
        flags_good.append(f"P K%: {k_rate:.0f}% (below avg)")
    elif k_rate >= KRATE_AVG:
        raw -= w["p_k_rate_inv"] * 0.2
        flags_bad.append(f"P K%: {k_rate:.0f}% (high)")

    woba = pm["p_woba_allowed"]
    if woba >= WOBA_VULNERABLE:
        raw += w["p_woba_allowed"]
        flags_good.append(f"wOBA allowed: {woba:.3f} (vulnerable)")
    elif woba >= WOBA_HITTABLE:
        raw += w["p_woba_allowed"] * 0.55
        flags_good.append(f"wOBA allowed: {woba:.3f}")
    elif woba >= WOBA_AVG:
        raw += w["p_woba_allowed"] * 0.2
    else:
        raw -= w["p_woba_allowed"] * 0.2

    # ── 3. Matchup ───────────────────────────────────────────────────────
    bvp_ab = bvp.get("bvp_ab") or 0
    bvp_avg = bvp.get("bvp_avg") or 0
    if bvp_ab >= 10 and bvp_avg >= 0.350:
        raw += w["bvp_history"]
        flags_good.append(f"BvP: {bvp.get('bvp_hits')}/{bvp_ab} ({bvp_avg:.3f})")
    elif bvp_ab >= 5 and bvp_avg >= 0.300:
        raw += w["bvp_history"] * 0.5
        flags_good.append(f"BvP: {bvp.get('bvp_hits')}/{bvp_ab} ({bvp_avg:.3f})")
    elif bvp_ab >= 10 and bvp_avg < 0.150:
        raw -= w["bvp_history"] * 0.3
        flags_bad.append(f"BvP: {bvp.get('bvp_hits')}/{bvp_ab} ({bvp_avg:.3f}) (struggles)")

    # Platoon edge: batter vs opposite-hand pitcher
    bat_side = prop.get("_bat_side", "R")
    pitcher_hand = prop.get("_pitcher_hand", "R")
    p_hand_char = "L" if str(pitcher_hand).upper() in ("L", "LHP") else "R"
    has_platoon = (bat_side != p_hand_char)
    if has_platoon:
        raw += w["platoon_edge"]
        flags_good.append("Platoon edge")

    # Arsenal contact rate (low whiff pitcher)
    ac = pm.get("arsenal_contact_rate", 75.0)
    if ac >= 82:
        raw += w["arsenal_contact"]
        flags_good.append(f"Arsenal contact: {ac:.0f}% (hittable)")
    elif ac >= 78:
        raw += w["arsenal_contact"] * 0.4

    # ── 3b. Weak spot (pitcher vulnerable at batter's lineup position) ──
    if weak_spot:
        raw += w.get("weak_spot", 6.0)
        ws_avg = weak_spot.get("ws_avg", 0)
        ws_hits = weak_spot.get("ws_hits", 0)
        ws_ab = weak_spot.get("ws_at_bats", 0)
        flags_good.append(f"Weak spot: {ws_hits}/{ws_ab} ({ws_avg:.3f}) at #{weak_spot.get('ws_batting_order')}")

    # ── 4. PropFinder consensus signals ──────────────────────────────────
    pf_rating = sf(prop.get("pf_rating"))
    if pf_rating > 0:
        if pf_rating >= 80:
            raw += w["pf_rating"]
            flags_good.append(f"PF Rating: {pf_rating:.0f}")
        elif pf_rating >= 60:
            raw += w["pf_rating"] * 0.5
        elif pf_rating < 40:
            raw -= w["pf_rating"] * 0.2
            flags_bad.append(f"PF Rating: {pf_rating:.0f} (low)")

    hr_l10 = parse_hit_rate(prop.get("hit_rate_l10"))
    hr_season = parse_hit_rate(prop.get("hit_rate_season"))
    hr_vs_team = parse_hit_rate(prop.get("hit_rate_vs_team"))

    if hr_l10 is not None:
        if hr_l10 >= 0.7:
            raw += w["hit_rate_l10"]
            flags_good.append(f"L10: {prop['hit_rate_l10']}")
        elif hr_l10 >= 0.5:
            raw += w["hit_rate_l10"] * 0.4

    if hr_season is not None:
        factor = 1.0 if hr_season >= 0.6 else 0.3
        raw += w["hit_rate_season"] * factor

    if hr_vs_team is not None:
        factor = 1.0 if hr_vs_team >= 0.6 else 0.3
        raw += w["hit_rate_vs_team"] * factor

    # ── 5. Recent form vs line ───────────────────────────────────────────
    avg_l10 = sf(prop.get("avg_l10"))
    line = sf(prop.get("line"))

    if avg_l10 > 0 and line > 0:
        gap = avg_l10 - line
        if gap >= 0.5:
            raw += w["avg_l10_vs_line"]
            flags_good.append(f"L10 avg: {avg_l10:.1f} vs line {line:.1f}")
        elif gap >= 0:
            raw += w["avg_l10_vs_line"] * 0.4
        elif gap < -0.5:
            raw -= w["avg_l10_vs_line"] * 0.3
            flags_bad.append(f"L10 avg: {avg_l10:.1f} below line {line:.1f}")

    # ── 6. Game environment ──────────────────────────────────────────────
    game_pk = prop.get("game_pk")
    gw = game_ctx.get(game_pk, {})
    vegas_total = sf(gw.get("over_under"))
    ballpark = gw.get("ballpark_name", "")

    if vegas_total > 0:
        if vegas_total >= 9.0:
            raw += w["vegas_total"]
            flags_good.append(f"Vegas total: {vegas_total:.1f} (high)")
        elif vegas_total <= 7.0:
            raw -= w["vegas_total"] * 0.3

    # ── 7. Streak bonus ──────────────────────────────────────────────────
    streak = si(prop.get("streak"))
    if streak >= 4:
        raw += w["streak"]
        flags_good.append(f"Hit streak: {streak} games")
    elif streak <= -3:
        raw -= w["streak"] * 0.5
        flags_bad.append(f"Hitless streak: {abs(streak)} games")

    # ── Normalize to 0-100 ───────────────────────────────────────────────
    max_possible = sum(w.values())
    score = round(max(0.0, min(100.0, (raw / max_possible) * 100)), 1)

    if score >= GRADE_FIRE:
        grade = "FIRE"
    elif score >= GRADE_STRONG:
        grade = "STRONG"
    elif score >= GRADE_LEAN:
        grade = "LEAN"
    else:
        grade = "SKIP"

    # Build why text
    why_parts = []
    if flags_good:
        why_parts.append("1+ hit: " + "; ".join(flags_good[:5]))
    if flags_bad:
        why_parts.append("Concerns: " + "; ".join(flags_bad[:3]))
    why = ". ".join(why_parts) + "." if why_parts else ""

    combined = {
        **bm,
        **pm,
        "bvp_ab": bvp.get("bvp_ab"),
        "bvp_hits": bvp.get("bvp_hits"),
        "bvp_avg": bvp.get("bvp_avg"),
        "platoon_edge": has_platoon,
        "game_total": vegas_total,
        "ballpark_name": ballpark,
        **(weak_spot or {}),
    }

    return score, grade, why, json.dumps(flags_good + flags_bad), combined


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    log.info("Starting Hit Pulse Score model for %s", TODAY)

    hit_props = load_hit_props()
    hit_data_map = load_hit_data()
    splits_map = load_splits()
    pitcher_map = load_pitcher_matchups()
    pitch_log_map = load_pitch_log()
    game_ctx = load_game_weather()
    matchups = load_today_matchups()

    batting_pos_map = load_batting_positions()
    pvbo_map = load_pitcher_vs_batting_order()

    # Load learned weights or fall back to baseline
    learned = load_learned_weights()
    weights = learned if learned else BASELINE_WEIGHTS
    log.info("Using %s hit weights", "learned" if learned else "baseline")

    if not hit_props:
        log.warning("No hit props found for today — did ingest run?")
        return

    log.info("Loaded %s hit props, %s matchups", len(hit_props), len(matchups))

    # Build pitcher lookup: batter_id → (pitcher_id, pitcher_hand, pitcher_name)
    batter_pitcher = {}
    for m in matchups:
        batter_pitcher[m["batter_id"]] = m

    # Fetch BvP for all matchups
    bvp_matchups = [
        {"batter_id": bid, "pitcher_id": batter_pitcher[bid]["pitcher_id"]}
        for bid in hit_props if bid in batter_pitcher
    ]
    bvp_map = load_bvp_bulk(bvp_matchups)

    output_rows = []
    seen = set()

    for batter_id, prop in hit_props.items():
        matchup = batter_pitcher.get(batter_id)
        if not matchup:
            continue

        if batter_id in seen:
            continue
        seen.add(batter_id)

        pitcher_id = matchup["pitcher_id"]
        bat_side = matchup["bat_side"]
        pitcher_hand = matchup["pitcher_hand"]

        hit_events = hit_data_map.get(batter_id, [])
        batter_splits = splits_map.get(batter_id, {})

        batter_metrics = compute_batter_contact_metrics(
            batter_id, bat_side, pitcher_hand, hit_events, batter_splits
        )
        pitcher_metrics = compute_pitcher_vulnerability(
            pitcher_id, bat_side,
            pitcher_map.get(pitcher_id, {}),
            pitch_log_map.get(pitcher_id, []),
        )

        bvp = bvp_map.get((batter_id, pitcher_id), {"bvp_ab": None, "bvp_hits": None, "bvp_avg": None})

        # Inject matchup info into prop for scoring
        prop["_bat_side"] = bat_side
        prop["_pitcher_hand"] = pitcher_hand

        # Check weak spot: pitcher vulnerable at batter's lineup position
        batting_pos = batting_pos_map.get((prop.get("game_pk"), batter_id))
        weak_spot = check_hit_weak_spot(pitcher_id, batting_pos, pvbo_map)

        score, grade, why, flags, metrics = score_hit_prop(
            batter_metrics, pitcher_metrics, bvp, prop, game_ctx, weights, weak_spot=weak_spot
        )

        gpk = prop.get("game_pk")
        row = {
            "run_date": TODAY.isoformat(),
            "run_timestamp": NOW.isoformat(),
            "game_pk": gpk,
            "game_date": gd.isoformat() if (gd := game_ctx.get(gpk, {}).get("game_date")) else None,
            "batter_id": batter_id,
            "batter_name": prop.get("batter_name", matchup.get("batter_name", "")),
            "bat_side": bat_side,
            "pitcher_id": pitcher_id,
            "pitcher_name": matchup.get("pitcher_name", ""),
            "pitcher_hand": pitcher_hand,
            "team_code": prop.get("team_code", ""),
            "opp_team_code": prop.get("opp_team_code", ""),
            "line": sf(prop.get("line")),
            "side": "OVER",
            "best_price": si(prop.get("best_price")),
            "best_book": prop.get("best_book", ""),
            "deep_link_desktop": prop.get("deep_link_desktop", ""),
            "deep_link_ios": prop.get("deep_link_ios", ""),
            "dk_outcome_code": prop.get("dk_outcome_code"),
            "dk_event_id": prop.get("dk_event_id"),
            "fd_market_id": prop.get("fd_market_id"),
            "fd_selection_id": prop.get("fd_selection_id"),
            # Batter metrics
            "batting_avg_vs_hand": metrics.get("batting_avg_vs_hand"),
            "contact_rate": metrics.get("contact_rate"),
            "l15_hit_rate": metrics.get("l15_hit_rate"),
            "l15_avg": metrics.get("l15_avg"),
            "hard_hit_pct": metrics.get("hard_hit_pct"),
            "ground_ball_pct": metrics.get("ground_ball_pct"),
            "line_drive_pct": metrics.get("line_drive_pct"),
            # Pitcher metrics
            "p_whip": metrics.get("p_whip"),
            "p_k_rate": metrics.get("p_k_rate"),
            "p_woba_allowed": metrics.get("p_woba_allowed"),
            "p_hard_hit_allowed": metrics.get("p_hard_hit_allowed"),
            "p_hits_per_9": metrics.get("p_hits_per_9"),
            # Matchup
            "bvp_ab": metrics.get("bvp_ab"),
            "bvp_hits": metrics.get("bvp_hits"),
            "bvp_avg": metrics.get("bvp_avg"),
            "platoon_edge": metrics.get("platoon_edge"),
            # PF signals
            "pf_rating": sf(prop.get("pf_rating")),
            "matchup_value": sf(prop.get("matchup_value")),
            "avg_l10": sf(prop.get("avg_l10")),
            "avg_home_away": sf(prop.get("avg_home_away")),
            "avg_vs_opponent": sf(prop.get("avg_vs_opponent")),
            "hit_rate_l10": prop.get("hit_rate_l10", ""),
            "hit_rate_season": prop.get("hit_rate_season", ""),
            "hit_rate_vs_team": prop.get("hit_rate_vs_team", ""),
            "streak": si(prop.get("streak")),
            # Context
            "game_total": metrics.get("game_total"),
            "ballpark_name": metrics.get("ballpark_name", ""),
            # Weak spot
            "batting_order_pos": batting_pos,
            **(weak_spot or {"ws_batting_order": None, "ws_at_bats": None, "ws_hits": None, "ws_avg": None}),
            # Score
            "score": score,
            "grade": grade,
            "why": why,
            "flags": flags,
        }
        output_rows.append(row)

    output_rows.sort(key=lambda r: r["score"], reverse=True)
    log.info("Scored %s hit props", len(output_rows))

    if output_rows:
        errors = bq.insert_rows_json(f"{PROJECT}.{DATASET}.hit_picks_daily", output_rows)
        if errors:
            log.error("BQ insert errors: %s", errors[:3])
        else:
            log.info("Wrote %s hit picks to hit_picks_daily", len(output_rows))

    # Print summary
    print(f"\n{'='*70}")
    print(f"HIT PULSE SCORE — {TODAY}")
    print(f"{'='*70}")
    for label in ["FIRE", "STRONG", "LEAN", "SKIP"]:
        group = [r for r in output_rows if r["grade"] == label]
        if not group:
            continue
        print(f"\n-- {label} ({len(group)}) --")
        for r in group:
            print(
                f"  {r['batter_name']:<22} ({r['team_code']} vs {r['opp_team_code']})  "
                f"Hit-Pulse: {r['score']:.0f}  ({r['best_price']:+d} {r['best_book']})"
            )
            print(
                f"    AVG:{r['batting_avg_vs_hand']:.3f}  Contact:{r['contact_rate']:.0f}%  "
                f"L15:{r['l15_hit_rate']:.3f}  "
                f"WHIP:{r['p_whip']:.2f}  "
                f"BvP:{r['bvp_hits'] or '?'}/{r['bvp_ab'] or '?'}"
            )

    skip_count = sum(1 for r in output_rows if r["grade"] == "SKIP")
    print(f"\n-- SKIP: {skip_count} props did not meet criteria --")


if __name__ == "__main__":
    main()
