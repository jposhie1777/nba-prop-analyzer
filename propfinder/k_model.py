# propfinder/k_model.py

"""
k_model.py — K Pulse Score: pitcher strikeout prop scoring system.

Scores OVER and UNDER K props on a 0-100 scale.
Self-learning: loads calibrated weights from k_model_weights table
and falls back to baseline weights when no learned data exists.

Grades:
  FIRE   (80+)  — strongest signal
  STRONG (65-79) — high confidence
  LEAN   (50-64) — moderate edge
  SKIP   (<50)   — no actionable edge

Factors scored:
  1. Pitcher K-rate dominance (K/9, K%, strike%)
  2. Pitch arsenal whiff power (avg whiff%, high-whiff pitch count)
  3. Opponent team K vulnerability (team K rank)
  4. PropFinder consensus signals (pf_rating, hit rates, streak)
  5. Recent form vs line (avg_l10 vs line, avg_vs_opponent)
  6. Game environment (Vegas total → more PAs, home/away splits)
  7. Line value (projection vs line gap)
"""

import datetime
import json
import logging
from collections import defaultdict
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
    # Pitcher K dominance
    "k_per_9":              12.0,
    "k_pct":                10.0,
    "strike_pct":            5.0,
    # Arsenal whiff power
    "arsenal_whiff":        10.0,
    "high_whiff_pitches":    8.0,
    # Opponent vulnerability
    "opp_k_rank":           10.0,
    # PropFinder signals
    "pf_rating":             8.0,
    "hit_rate_l10":          7.0,
    "hit_rate_season":       5.0,
    "hit_rate_vs_team":      5.0,
    # Recent form vs line
    "avg_l10_vs_line":      10.0,
    "avg_vs_opp_vs_line":    5.0,
    # Game context
    "vegas_total":           3.0,
    # Streak
    "streak":                2.0,
}

# Thresholds for pitcher K dominance
K9_ELITE = 10.5
K9_STRONG = 9.0
K9_AVG = 7.5
K9_WEAK = 6.0

KPCT_ELITE = 28.0
KPCT_STRONG = 24.0
KPCT_AVG = 20.0

WHIFF_ELITE = 30.0
WHIFF_STRONG = 25.0
WHIFF_AVG = 20.0

# Grade thresholds
GRADE_FIRE = 80
GRADE_STRONG = 65
GRADE_LEAN = 50


# ── Data loaders ─────────────────────────────────────────────────────────

def load_k_props():
    """Load today's K props (over + under) from raw_k_props."""
    rows = query(f"""
        SELECT *
        FROM {tbl('raw_k_props')}
        WHERE run_date = '{TODAY}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pitcher_id, over_under
            ORDER BY ingested_at DESC
        ) = 1
    """)
    # Group by pitcher_id → {over: row, under: row}
    out = defaultdict(dict)
    for r in rows:
        out[r["pitcher_id"]][r["over_under"]] = r
    return out


def load_pitcher_matchups():
    """Load pitcher matchup data with K-specific stats."""
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
    """Load pitch arsenal data — whiff rates and K% per pitch."""
    rows = query(f"""
        SELECT pitcher_id, pitch_name, percentage, whiff, k_percent
        FROM {tbl('raw_pitch_log')}
        WHERE run_date = '{TODAY}' AND season = {TODAY.year}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pitcher_id, pitch_name
            ORDER BY percentage DESC
        ) = 1
    """)
    out = defaultdict(list)
    for r in rows:
        out[r["pitcher_id"]].append(r)
    return out


def load_team_k_rankings():
    """Load team strikeout vulnerability rankings."""
    rows = query(f"""
        SELECT team_code, rank, value
        FROM {tbl('raw_team_strikeout_rankings')}
        WHERE run_date = '{TODAY}'
          AND category = 'strikeouts'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY team_code ORDER BY ingested_at DESC) = 1
    """)
    return {r["team_code"]: r for r in rows}


def load_game_weather():
    """Load game context (Vegas totals, ballpark)."""
    rows = query(f"""
        SELECT game_pk, over_under, ballpark_name
        FROM {tbl('raw_game_weather')}
        WHERE run_date = '{TODAY}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY ingested_at DESC) = 1
    """)
    return {r["game_pk"]: r for r in rows}


def load_learned_weights():
    """Load the most recent learned weights from k_model_weights."""
    try:
        rows = query(f"""
            SELECT factor, weight
            FROM {tbl('k_model_weights')}
            WHERE run_date = (
                SELECT MAX(run_date) FROM {tbl('k_model_weights')}
            )
        """)
        if rows:
            weights = {r["factor"]: r["weight"] for r in rows}
            log.info("Loaded %s learned weights from k_model_weights", len(weights))
            return weights
    except Exception as exc:
        log.warning("Could not load learned weights: %s", exc)
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


# ── Scoring engine ───────────────────────────────────────────────────────

def compute_arsenal_metrics(pitches):
    """Compute aggregate whiff and K% from pitcher's arsenal."""
    if not pitches:
        return 0.0, 0.0, 0

    whiff_vals = []
    k_pct_vals = []
    high_whiff_count = 0

    for p in pitches:
        pct = sf(p.get("percentage"))
        if pct < 0.03:  # Skip pitches thrown < 3% of the time
            continue
        whiff = sf(p.get("whiff"))
        kpct = sf(p.get("k_percent"))
        if whiff > 0:
            whiff_vals.append(whiff)
        if kpct > 0:
            k_pct_vals.append(kpct)
        if whiff >= 25.0:
            high_whiff_count += 1

    avg_whiff = sum(whiff_vals) / len(whiff_vals) * 100 if whiff_vals else 0.0
    avg_kpct = sum(k_pct_vals) / len(k_pct_vals) * 100 if k_pct_vals else 0.0
    # Normalize — PropFinder whiff/k_percent may already be in 0-100 or 0-1
    if avg_whiff > 0 and avg_whiff < 1:
        avg_whiff *= 100
    if avg_kpct > 0 and avg_kpct < 1:
        avg_kpct *= 100

    return round(avg_whiff, 1), round(avg_kpct, 1), high_whiff_count


def score_k_prop(side, prop, pitcher_splits, pitches, team_k, game_ctx, weights):
    """
    Score a single K prop (over or under).
    Returns (score, grade, why, flags, metrics_dict).
    """
    w = weights
    raw = 0.0
    flags_good = []
    flags_bad = []

    line = sf(prop.get("line"))
    is_over = (side == "over")

    # ── 1. Pitcher K-rate dominance ──────────────────────────────────────
    season = pitcher_splits.get("Season", {})
    k9 = sf(season.get("strikeouts_per_9"))
    k_pct_raw = sf(season.get("k_pct"))
    k_pct = k_pct_raw if k_pct_raw > 1 else k_pct_raw * 100
    strike_pct_raw = sf(season.get("strike_pct"))
    strike_pct = strike_pct_raw if strike_pct_raw > 1 else strike_pct_raw * 100
    ip = sf(season.get("ip"))
    batters_faced = si(season.get("batters_faced"))

    if is_over:
        if k9 >= K9_ELITE:
            raw += w["k_per_9"]
            flags_good.append(f"K/9: {k9:.1f} (elite)")
        elif k9 >= K9_STRONG:
            raw += w["k_per_9"] * 0.7
            flags_good.append(f"K/9: {k9:.1f} (strong)")
        elif k9 >= K9_AVG:
            raw += w["k_per_9"] * 0.35
        else:
            raw -= w["k_per_9"] * 0.3
            flags_bad.append(f"K/9: {k9:.1f} (low)")

        if k_pct >= KPCT_ELITE:
            raw += w["k_pct"]
            flags_good.append(f"K%: {k_pct:.1f}% (elite)")
        elif k_pct >= KPCT_STRONG:
            raw += w["k_pct"] * 0.65
            flags_good.append(f"K%: {k_pct:.1f}%")
        elif k_pct >= KPCT_AVG:
            raw += w["k_pct"] * 0.3
        else:
            raw -= w["k_pct"] * 0.2
            flags_bad.append(f"K%: {k_pct:.1f}% (low)")

        if strike_pct >= 67:
            raw += w["strike_pct"]
            flags_good.append(f"Strike%: {strike_pct:.0f}%")
        elif strike_pct >= 63:
            raw += w["strike_pct"] * 0.5
    else:
        # UNDER: invert — low K rates are good
        if k9 < K9_WEAK:
            raw += w["k_per_9"]
            flags_good.append(f"K/9: {k9:.1f} (low K pitcher)")
        elif k9 < K9_AVG:
            raw += w["k_per_9"] * 0.6
            flags_good.append(f"K/9: {k9:.1f} (below avg)")
        elif k9 >= K9_ELITE:
            raw -= w["k_per_9"] * 0.4
            flags_bad.append(f"K/9: {k9:.1f} (high — risky under)")

        if k_pct < KPCT_AVG:
            raw += w["k_pct"] * 0.8
            flags_good.append(f"K%: {k_pct:.1f}% (low)")
        elif k_pct >= KPCT_STRONG:
            raw -= w["k_pct"] * 0.3
            flags_bad.append(f"K%: {k_pct:.1f}% (high)")

    # ── 2. Arsenal whiff power ───────────────────────────────────────────
    avg_whiff, avg_k_pct, high_whiff_n = compute_arsenal_metrics(pitches)

    if is_over:
        if avg_whiff >= WHIFF_ELITE:
            raw += w["arsenal_whiff"]
            flags_good.append(f"Arsenal whiff: {avg_whiff:.0f}% (elite)")
        elif avg_whiff >= WHIFF_STRONG:
            raw += w["arsenal_whiff"] * 0.6
            flags_good.append(f"Arsenal whiff: {avg_whiff:.0f}%")
        elif avg_whiff >= WHIFF_AVG:
            raw += w["arsenal_whiff"] * 0.25

        if high_whiff_n >= 3:
            raw += w["high_whiff_pitches"]
            flags_good.append(f"{high_whiff_n} high-whiff pitches")
        elif high_whiff_n >= 2:
            raw += w["high_whiff_pitches"] * 0.5
    else:
        if avg_whiff < WHIFF_AVG:
            raw += w["arsenal_whiff"] * 0.7
            flags_good.append(f"Arsenal whiff: {avg_whiff:.0f}% (weak)")
        elif avg_whiff >= WHIFF_ELITE:
            raw -= w["arsenal_whiff"] * 0.3
            flags_bad.append(f"Arsenal whiff: {avg_whiff:.0f}% (dangerous)")

        if high_whiff_n <= 1:
            raw += w["high_whiff_pitches"] * 0.5
            flags_good.append(f"Only {high_whiff_n} high-whiff pitch(es)")

    # ── 3. Opponent team K vulnerability ─────────────────────────────────
    opp_code = prop.get("opp_team_code", "")
    tk = team_k.get(opp_code, {})
    opp_rank = si(tk.get("rank"), default=None)
    opp_k_total = si(tk.get("value"), default=None)

    if opp_rank is not None:
        if is_over:
            if opp_rank <= 5:
                raw += w["opp_k_rank"]
                flags_good.append(f"Opponent K rank: #{opp_rank} (most Ks)")
            elif opp_rank <= 10:
                raw += w["opp_k_rank"] * 0.6
                flags_good.append(f"Opponent K rank: #{opp_rank}")
            elif opp_rank >= 25:
                raw -= w["opp_k_rank"] * 0.3
                flags_bad.append(f"Opponent K rank: #{opp_rank} (rarely Ks)")
        else:
            if opp_rank >= 25:
                raw += w["opp_k_rank"]
                flags_good.append(f"Opponent K rank: #{opp_rank} (low K team)")
            elif opp_rank >= 20:
                raw += w["opp_k_rank"] * 0.5
            elif opp_rank <= 5:
                raw -= w["opp_k_rank"] * 0.3
                flags_bad.append(f"Opponent K rank: #{opp_rank} (K-prone)")

    # ── 4. PropFinder consensus signals ──────────────────────────────────
    pf_rating = sf(prop.get("pf_rating"))
    if pf_rating > 0:
        if is_over:
            if pf_rating >= 4.0:
                raw += w["pf_rating"]
                flags_good.append(f"PF Rating: {pf_rating:.1f}/5")
            elif pf_rating >= 3.0:
                raw += w["pf_rating"] * 0.5
            elif pf_rating < 2.0:
                raw -= w["pf_rating"] * 0.2
                flags_bad.append(f"PF Rating: {pf_rating:.1f}/5 (low)")
        else:
            if pf_rating < 2.0:
                raw += w["pf_rating"] * 0.8
                flags_good.append(f"PF Rating: {pf_rating:.1f}/5 (weak over)")
            elif pf_rating >= 4.0:
                raw -= w["pf_rating"] * 0.3
                flags_bad.append(f"PF Rating: {pf_rating:.1f}/5 (strong over signal)")

    # Hit rates
    hr_l10 = parse_hit_rate(prop.get("hit_rate_l10"))
    hr_season = parse_hit_rate(prop.get("hit_rate_season"))
    hr_vs_team = parse_hit_rate(prop.get("hit_rate_vs_team"))

    if hr_l10 is not None:
        if is_over:
            if hr_l10 >= 0.7:
                raw += w["hit_rate_l10"]
                flags_good.append(f"L10 hit rate: {prop['hit_rate_l10']}")
            elif hr_l10 >= 0.5:
                raw += w["hit_rate_l10"] * 0.4
        else:
            if hr_l10 <= 0.3:
                raw += w["hit_rate_l10"]
                flags_good.append(f"L10 hit rate: {prop['hit_rate_l10']} (low)")
            elif hr_l10 <= 0.5:
                raw += w["hit_rate_l10"] * 0.4

    if hr_season is not None:
        factor = 1.0 if ((is_over and hr_season >= 0.6) or (not is_over and hr_season <= 0.4)) else 0.3
        raw += w["hit_rate_season"] * factor

    if hr_vs_team is not None:
        factor = 1.0 if ((is_over and hr_vs_team >= 0.6) or (not is_over and hr_vs_team <= 0.4)) else 0.3
        raw += w["hit_rate_vs_team"] * factor

    # ── 5. Recent form vs line ───────────────────────────────────────────
    avg_l10 = sf(prop.get("avg_l10"))
    avg_vs_opp = sf(prop.get("avg_vs_opponent"))

    if avg_l10 > 0 and line > 0:
        gap = avg_l10 - line
        if is_over:
            if gap >= 1.5:
                raw += w["avg_l10_vs_line"]
                flags_good.append(f"L10 avg: {avg_l10:.1f} (line {line:.1f}, +{gap:.1f})")
            elif gap >= 0.5:
                raw += w["avg_l10_vs_line"] * 0.6
                flags_good.append(f"L10 avg: {avg_l10:.1f} vs line {line:.1f}")
            elif gap < -1.0:
                raw -= w["avg_l10_vs_line"] * 0.3
                flags_bad.append(f"L10 avg: {avg_l10:.1f} below line {line:.1f}")
        else:
            if gap <= -1.0:
                raw += w["avg_l10_vs_line"]
                flags_good.append(f"L10 avg: {avg_l10:.1f} well below line {line:.1f}")
            elif gap <= 0:
                raw += w["avg_l10_vs_line"] * 0.5
                flags_good.append(f"L10 avg: {avg_l10:.1f} vs line {line:.1f}")
            elif gap >= 1.5:
                raw -= w["avg_l10_vs_line"] * 0.3
                flags_bad.append(f"L10 avg: {avg_l10:.1f} above line {line:.1f}")

    if avg_vs_opp > 0 and line > 0:
        gap_opp = avg_vs_opp - line
        threshold = 0.5 if is_over else -0.5
        if (is_over and gap_opp >= threshold) or (not is_over and gap_opp <= threshold):
            raw += w["avg_vs_opp_vs_line"]
            flags_good.append(f"vs Opp avg: {avg_vs_opp:.1f}")

    # ── 6. Game environment ──────────────────────────────────────────────
    game_pk = prop.get("game_pk")
    gw = game_ctx.get(game_pk, {})
    vegas_total = sf(gw.get("over_under"))
    ballpark = gw.get("ballpark_name", "")

    if vegas_total > 0:
        if is_over:
            if vegas_total >= 9.0:
                raw += w["vegas_total"]
                flags_good.append(f"Vegas total: {vegas_total:.1f} (high)")
            elif vegas_total <= 7.0:
                raw -= w["vegas_total"] * 0.3
        else:
            if vegas_total <= 7.0:
                raw += w["vegas_total"]
                flags_good.append(f"Vegas total: {vegas_total:.1f} (low — fewer PAs)")
            elif vegas_total >= 9.5:
                raw -= w["vegas_total"] * 0.3

    # ── 7. Streak bonus ──────────────────────────────────────────────────
    streak = si(prop.get("streak"))
    if streak >= 4 and is_over:
        raw += w["streak"]
        flags_good.append(f"Over streak: {streak} games")
    elif streak <= -3 and not is_over:
        raw += w["streak"]
        flags_good.append(f"Under streak: {abs(streak)} games")

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
    side_label = "OVER" if is_over else "UNDER"
    why_parts = []
    if flags_good:
        why_parts.append(f"{side_label} {line:.1f} Ks: " + "; ".join(flags_good[:4]))
    if flags_bad:
        why_parts.append("Concerns: " + "; ".join(flags_bad[:3]))
    why = ". ".join(why_parts) + "." if why_parts else ""

    metrics = {
        "k_per_9": round(k9, 2),
        "k_pct": round(k_pct, 1),
        "season_k_per_9": round(sf(season.get("strikeouts_per_9")), 2),
        "ip": round(ip, 1),
        "batters_faced": batters_faced,
        "strike_pct": round(strike_pct, 1),
        "arsenal_whiff_avg": round(avg_whiff, 1),
        "arsenal_k_pct_avg": round(avg_k_pct, 1),
        "num_high_whiff_pitches": high_whiff_n,
        "opp_team_k_rank": opp_rank,
        "opp_team_k_total": opp_k_total,
        "game_total": vegas_total,
        "ballpark_name": ballpark,
    }

    return score, grade, why, json.dumps(flags_good + flags_bad), metrics


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    log.info("Starting K Pulse Score model for %s", TODAY)

    k_props = load_k_props()
    pitcher_map = load_pitcher_matchups()
    pitch_log_map = load_pitch_log()
    team_k = load_team_k_rankings()
    game_ctx = load_game_weather()

    # Load learned weights or fall back to baseline
    learned = load_learned_weights()
    weights = learned if learned else BASELINE_WEIGHTS
    log.info("Using %s weights", "learned" if learned else "baseline")

    if not k_props:
        log.warning("No K props found for today — did ingest run?")
        return

    log.info("Processing K props for %s pitchers", len(k_props))

    output_rows = []
    for pitcher_id, sides in k_props.items():
        pitcher_splits = pitcher_map.get(pitcher_id, {})
        pitches = pitch_log_map.get(pitcher_id, [])

        for side in ("over", "under"):
            prop = sides.get(side)
            if not prop:
                continue

            score, grade, why, flags, metrics = score_k_prop(
                side, prop, pitcher_splits, pitches, team_k, game_ctx, weights
            )

            row = {
                "run_date": TODAY.isoformat(),
                "run_timestamp": NOW.isoformat(),
                "game_pk": prop.get("game_pk"),
                "pitcher_id": pitcher_id,
                "pitcher_name": prop.get("pitcher_name", ""),
                "pitcher_hand": (pitcher_splits.get("Season", {}).get("pitcher_hand") or ""),
                "team_code": prop.get("team_code", ""),
                "opp_team_code": prop.get("opp_team_code", ""),
                "line": sf(prop.get("line")),
                "side": side.upper(),
                "best_price": si(prop.get("best_price")),
                "best_book": prop.get("best_book", ""),
                "deep_link_desktop": prop.get("deep_link_desktop", ""),
                "deep_link_ios": prop.get("deep_link_ios", ""),
                "pf_rating": sf(prop.get("pf_rating")),
                "avg_l10": sf(prop.get("avg_l10")),
                "avg_home_away": sf(prop.get("avg_home_away")),
                "avg_vs_opponent": sf(prop.get("avg_vs_opponent")),
                "hit_rate_l10": prop.get("hit_rate_l10", ""),
                "hit_rate_season": prop.get("hit_rate_season", ""),
                "hit_rate_vs_team": prop.get("hit_rate_vs_team", ""),
                "streak": si(prop.get("streak")),
                **metrics,
                "score": score,
                "grade": grade,
                "why": why,
                "flags": flags,
            }
            output_rows.append(row)

    output_rows.sort(key=lambda r: r["score"], reverse=True)
    log.info("Scored %s K props", len(output_rows))

    if output_rows:
        errors = bq.insert_rows_json(f"{PROJECT}.{DATASET}.k_picks_daily", output_rows)
        if errors:
            log.error("BQ insert errors: %s", errors[:3])
        else:
            log.info("Wrote %s K picks to k_picks_daily", len(output_rows))

    # Print summary
    print(f"\n{'='*70}")
    print(f"K PULSE SCORE — {TODAY}")
    print(f"{'='*70}")
    for label in ["FIRE", "STRONG", "LEAN", "SKIP"]:
        group = [r for r in output_rows if r["grade"] == label]
        if not group:
            continue
        print(f"\n-- {label} --")
        for r in group:
            side = r["side"]
            emoji = "\U0001f525" if side == "OVER" else "\u2744\ufe0f"
            print(
                f"  {emoji} {r['pitcher_name']:<22} {side} {r['line']:.1f} Ks  "
                f"K-Pulse: {r['score']:.0f}  ({r['best_price']:+d} {r['best_book']})"
            )
            print(
                f"    K/9:{r['k_per_9']:.1f}  K%:{r['k_pct']:.0f}%  "
                f"Whiff:{r['arsenal_whiff_avg']:.0f}%  "
                f"OppK#{r['opp_team_k_rank'] or '?'}  "
                f"L10:{r['avg_l10']:.1f}"
            )
            if r["why"]:
                print(f"    {r['why'][:120]}")

    skip_count = sum(1 for r in output_rows if r["grade"] == "SKIP")
    print(f"\n-- SKIP: {skip_count} props did not meet criteria --")


if __name__ == "__main__":
    main()
