"""
model.py — PropFinder HR scoring model
Reads raw BigQuery tables for today, computes metrics, scores each
batter/pitcher matchup, and writes results to hr_picks_daily.
"""

import datetime
import json
import logging
import math
from collections import defaultdict
from statistics import mean
from typing import Optional

from google.cloud import bigquery

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT = "graphite-flare-477419-h7"
DATASET = "propfinder"
TODAY   = datetime.date.today()
NOW     = datetime.datetime.utcnow()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bq = bigquery.Client(project=PROJECT)

def tbl(name: str) -> str:
    return f"`{PROJECT}.{DATASET}.{name}`"

# ── Thresholds (your criteria) ────────────────────────────────────────────────
# Batter
L15_BARREL_THRESHOLD     = 20.0   # %
ISO_THRESHOLD            = 0.200
HR_FB_THRESHOLD          = 10.0   # %
L15_EV_GOOD              = 90.0   # mph — Favorable
L15_EV_ELITE             = 95.0   # mph — Elite
SEASON_BARREL_THRESHOLD  = 10.0   # %

# Pitcher
P_HR9_THRESHOLD          = 1.2
P_BARREL_THRESHOLD       = 10.0   # %
P_HR_FB_THRESHOLD        = 12.0   # %

# Grade cutoffs (out of 10)
IDEAL_MIN     = 7.0
FAVORABLE_MIN = 4.5
FLIER_MIN     = 2.5

# ── Query helpers ─────────────────────────────────────────────────────────────
def query(sql: str) -> list[dict]:
    rows = bq.query(sql).result()
    return [dict(r) for r in rows]

# ── Load today's raw data ─────────────────────────────────────────────────────
def load_hit_data() -> dict[int, list[dict]]:
    """Returns {batter_id: [events sorted newest→oldest]}"""
    rows = query(f"""
        SELECT *
        FROM {tbl('raw_hit_data')}
        WHERE run_date = '{TODAY}'
        ORDER BY batter_id, event_date DESC
    """)
    out: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        out[r["batter_id"]].append(r)
    return out

def load_splits() -> dict[int, dict[str, dict]]:
    """Returns {batter_id: {split_code: row}}"""
    rows = query(f"""
        SELECT *
        FROM {tbl('raw_splits')}
        WHERE run_date = '{TODAY}'
    """)
    out: dict[int, dict] = defaultdict(dict)
    for r in rows:
        out[r["batter_id"]][r["split_code"]] = r
    return out

def load_pitcher_matchup() -> dict[int, dict[str, dict]]:
    """Returns {pitcher_id: {split: row}} — Season/vsLHB/vsRHB"""
    rows = query(f"""
        SELECT *
        FROM {tbl('raw_pitcher_matchup')}
        WHERE run_date = '{TODAY}'
    """)
    out: dict[int, dict] = defaultdict(dict)
    for r in rows:
        out[r["pitcher_id"]][r["split"]] = r
    return out

def load_pitch_log() -> dict[int, list[dict]]:
    """Returns {pitcher_id: [pitch log rows for 2025]}"""
    rows = query(f"""
        SELECT *
        FROM {tbl('raw_pitch_log')}
        WHERE run_date = '{TODAY}' AND season = 2025
        ORDER BY pitcher_id, percentage DESC
    """)
    out: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        out[r["pitcher_id"]].append(r)
    return out

def load_games() -> list[dict]:
    """Get today's game/pitcher/batter assignments from raw_hit_data + raw_pitcher_matchup."""
    rows = query(f"""
        SELECT DISTINCT
            h.game_pk,
            h.batter_id,
            h.batter_name,
            h.bat_side,
            pm.pitcher_id,
            pm.pitcher_name,
            pm.pitcher_hand,
            pm.opp_team_id
        FROM {tbl('raw_hit_data')} h
        JOIN {tbl('raw_pitcher_matchup')} pm
            ON h.game_pk = pm.game_pk
            AND pm.run_date = '{TODAY}'
        WHERE h.run_date = '{TODAY}'
    """)
    return rows

def load_game_meta() -> dict[int, dict]:
    """Load home/away team names from MLB API cached in pitcher matchup."""
    rows = query(f"""
        SELECT DISTINCT game_pk, pitcher_id, pitcher_name, opp_team_id
        FROM {tbl('raw_pitcher_matchup')}
        WHERE run_date = '{TODAY}'
    """)
    # We'll derive home/away from the ingest data
    meta: dict[int, dict] = {}
    for r in rows:
        gp = r["game_pk"]
        if gp not in meta:
            meta[gp] = {"pitchers": []}
        meta[gp]["pitchers"].append(r)
    return meta

# ── Batter metric computations ────────────────────────────────────────────────
def compute_batter_metrics(
    batter_id: int,
    bat_side: str,
    pitcher_hand: str,
    hit_events: list[dict],
    splits: dict[str, dict],
    pitcher_pitch_log: list[dict],
) -> dict:
    """Compute all batter-side metrics for the HR model."""

    # Pitcher's primary pitch types (top by usage, current season, vs batter hand)
    batter_hand_code = "LHB" if bat_side == "L" else "RHB"
    primary_pitches = {
        p["pitch_name"]
        for p in pitcher_pitch_log
        if p["batter_hand"] == batter_hand_code
        and (p.get("percentage") or 0) > 0.05
    }

    # Filter hit events by pitcher handedness + pitch mix
    filtered = [
        ev for ev in hit_events
        if ev["pitch_hand"] == pitcher_hand
        and (not primary_pitches or ev["pitch_type"] in primary_pitches)
    ]

    # L15 metrics (filtered)
    l15 = filtered[:15]
    l15_count = len(l15)
    l15_barrel_pct = 0.0
    l15_ev         = 0.0
    l15_hh_pct     = 0.0
    if l15_count > 0:
        l15_barrel_pct = sum(1 for e in l15 if e.get("is_barrel")) / l15_count * 100
        l15_ev         = mean(e["launch_speed"] for e in l15)
        l15_hh_pct     = sum(1 for e in l15 if e.get("launch_speed", 0) >= 95) / l15_count * 100

    # Season metrics (2025, all events)
    season_events = [e for e in hit_events if e.get("season") == 2025]
    season_count  = len(season_events)
    season_barrel_pct = 0.0
    season_ev         = 0.0
    if season_count > 0:
        season_barrel_pct = sum(1 for e in season_events if e.get("is_barrel")) / season_count * 100
        season_ev         = mean(e["launch_speed"] for e in season_events)

    # HR/FB% from hit events
    flyballs = [e for e in hit_events if e.get("trajectory") == "fly_ball"]
    hr_fb_pct = 0.0
    if flyballs:
        hr_fb_pct = sum(1 for e in flyballs if e["result"] == "home_run") / len(flyballs) * 100

    # ISO + SLG from splits (vs LHP or vs RHP)
    split_key = "vl" if pitcher_hand == "L" else "vr"
    split = splits.get(split_key, splits.get("r", {}))
    iso  = 0.0
    slg  = 0.0
    if split:
        ab  = split.get("at_bats") or 0
        hr  = split.get("home_runs") or 0
        dbl = split.get("doubles") or 0
        tri = split.get("triples") or 0
        if ab > 0:
            iso = (dbl + 2 * tri + 3 * hr) / ab
            slg = split.get("slg") or 0.0

    return {
        "iso":               round(iso, 3),
        "slg":               round(float(slg), 3),
        "l15_ev":            round(l15_ev, 1),
        "l15_barrel_pct":    round(l15_barrel_pct, 1),
        "season_ev":         round(season_ev, 1),
        "season_barrel_pct": round(season_barrel_pct, 1),
        "l15_hard_hit_pct":  round(l15_hh_pct, 1),
        "hr_fb_pct":         round(hr_fb_pct, 1),
    }

# ── Pitcher metric computations ───────────────────────────────────────────────
def compute_pitcher_metrics(
    pitcher_id: int,
    bat_side: str,
    matchup_splits: dict[str, dict],
    pitch_log: list[dict],
) -> dict:
    """Compute pitcher-side metrics for the HR model."""
    batter_hand_str = "vsLHB" if bat_side == "L" else "vsRHB"

    season_split = matchup_splits.get("Season", {})
    hand_split   = matchup_splits.get(batter_hand_str, {})

    p_hr9_season  = float(season_split.get("hr_per_9") or 0)
    p_hr9_vs_hand = float(hand_split.get("hr_per_9") or 0)
    p_barrel_pct  = float(season_split.get("barrel_pct") or 0) * 100 if (season_split.get("barrel_pct") or 0) < 1 else float(season_split.get("barrel_pct") or 0)
    p_fb_pct      = float(season_split.get("fb_pct") or 0) * 100 if (season_split.get("fb_pct") or 0) < 1 else float(season_split.get("fb_pct") or 0)
    p_hr_vs_hand  = int(hand_split.get("home_runs") or 0)

    # HR/FB% — computed from HR and FB%
    # HR/FB = HR / (IP * FB_per_inning) — approximate from HR/9 and FB%
    p_hr_fb_pct = 0.0
    if p_fb_pct > 0 and p_hr9_season > 0:
        # rough: HR/9 / (FB% * ~3.5 FB per inning per 9) 
        p_hr_fb_pct = (p_hr9_season / (p_fb_pct / 100 * 3.5)) * 100
        p_hr_fb_pct = min(p_hr_fb_pct, 50.0)  # cap at 50

    # Pitcher's L25 barrel pct — from pitch_log events allowed
    batter_hand_code = "LHB" if bat_side == "L" else "RHB"
    hand_pitches = [p for p in pitch_log if p["batter_hand"] == batter_hand_code]
    # Use season barrel from splits as primary, pitch_log as secondary signal

    return {
        "p_hr9_season":    round(p_hr9_season, 2),
        "p_hr9_vs_hand":   round(p_hr9_vs_hand, 2),
        "p_barrel_pct":    round(p_barrel_pct, 1),
        "p_hr_fb_pct":     round(p_hr_fb_pct, 1),
        "p_hr_vs_hand":    p_hr_vs_hand,
        "p_fb_pct":        round(p_fb_pct, 1),
    }

# ── Scoring model ─────────────────────────────────────────────────────────────
def score_matchup(bm: dict, pm: dict) -> tuple[float, list[str], list[str]]:
    """
    Returns (score 0-10, flags_good, flags_bad)
    Score is weighted sum of criteria checks.
    """
    score = 0.0
    flags_good: list[str] = []
    flags_bad:  list[str] = []

    # ── Batter checks (max 5 points) ─────────────────────────────────────────
    # 1. L15 Barrel% vs pitcher handedness + pitch mix (weight: 1.5)
    if bm["l15_barrel_pct"] >= L15_BARREL_THRESHOLD:
        score += 1.5
        flags_good.append(f"L15 Barrel {bm['l15_barrel_pct']:.1f}%")
    elif bm["l15_barrel_pct"] >= 12.0:
        score += 0.75
        flags_good.append(f"L15 Barrel {bm['l15_barrel_pct']:.1f}% (Avg)")

    # 2. ISO (weight: 1.5)
    if bm["iso"] >= ISO_THRESHOLD:
        score += 1.5
        flags_good.append(f"ISO {bm['iso']:.3f}")
    elif bm["iso"] >= 0.150:
        score += 0.75

    # 3. HR/FB% (weight: 1.0)
    if bm["hr_fb_pct"] >= HR_FB_THRESHOLD:
        score += 1.0
        flags_good.append(f"HR/FB {bm['hr_fb_pct']:.1f}%")

    # 4. L15 EV (weight: 0.5 / 1.0)
    if bm["l15_ev"] >= L15_EV_ELITE:
        score += 1.0
        flags_good.append(f"L15 EV {bm['l15_ev']:.1f} (Elite)")
    elif bm["l15_ev"] >= L15_EV_GOOD:
        score += 0.5
        flags_good.append(f"L15 EV {bm['l15_ev']:.1f}")

    # 5. Season Barrel% (weight: 0.5)
    if bm["season_barrel_pct"] >= SEASON_BARREL_THRESHOLD:
        score += 0.5
        flags_good.append(f"'25 Barrel {bm['season_barrel_pct']:.1f}%")

    # ── Pitcher checks (max 5 points) ────────────────────────────────────────
    # 6. Pitcher HR/9 vs batter handedness (weight: 2.0)
    if pm["p_hr9_vs_hand"] >= P_HR9_THRESHOLD:
        score += 2.0
        flags_good.append(f"HR/9 vs hand {pm['p_hr9_vs_hand']:.2f}")
    elif pm["p_hr9_vs_hand"] >= 0.8:
        score += 1.0
        flags_good.append(f"HR/9 vs hand {pm['p_hr9_vs_hand']:.2f} (Mod)")
    elif pm["p_hr9_vs_hand"] > 0 and pm["p_hr9_vs_hand"] < 0.5:
        flags_bad.append(f"HR/9 vs hand {pm['p_hr9_vs_hand']:.2f} (Low)")

    # 7. Pitcher Barrel% allowed (weight: 1.5)
    if pm["p_barrel_pct"] >= P_BARREL_THRESHOLD:
        score += 1.5
        flags_good.append(f"P Barrel% {pm['p_barrel_pct']:.1f}%")
    elif pm["p_barrel_pct"] >= 7.0:
        score += 0.75

    # 8. Pitcher HR/FB% (weight: 1.0)
    if pm["p_hr_fb_pct"] >= P_HR_FB_THRESHOLD:
        score += 1.0
        flags_good.append(f"P HR/FB {pm['p_hr_fb_pct']:.1f}%")

    # 9. HR allowed vs this handedness (weight: 0.5)
    if pm["p_hr_vs_hand"] >= 8:
        score += 0.5
        flags_good.append(f"{pm['p_hr_vs_hand']} HR vs this hand")
    elif pm["p_hr_vs_hand"] == 0 and pm["p_hr9_vs_hand"] < 0.5:
        flags_bad.append("0 HR vs this hand")

    return round(score, 2), flags_good, flags_bad

def grade_matchup(score: float, flags_bad: list[str]) -> str:
    if score >= IDEAL_MIN and not flags_bad:
        return "IDEAL"
    elif score >= IDEAL_MIN:
        return "FAVORABLE"
    elif score >= FAVORABLE_MIN:
        return "FAVORABLE"
    elif score >= FLIER_MIN:
        return "FLIER"
    else:
        return "SKIP"

def build_why(batter_name: str, bat_side: str, pitcher_name: str, pitcher_hand: str,
              bm: dict, pm: dict, grade: str, flags_good: list, flags_bad: list) -> str:
    parts = []
    hand_str = "LHB" if bat_side == "L" else "RHB"
    p_hand_str = "LHP" if pitcher_hand == "L" else "RHP"

    if grade in ("IDEAL", "FAVORABLE"):
        if bat_side != pitcher_hand:
            parts.append(f"{hand_str} platoon advantage vs {p_hand_str} {pitcher_name}")
        if bm["l15_barrel_pct"] >= L15_BARREL_THRESHOLD:
            parts.append(f"Elite L15 Barrel% of {bm['l15_barrel_pct']:.1f}%")
        if bm["iso"] >= ISO_THRESHOLD:
            parts.append(f"Strong ISO of {bm['iso']:.3f}")
        if pm["p_hr9_vs_hand"] >= P_HR9_THRESHOLD:
            parts.append(f"{pitcher_name} gives up HR/9 of {pm['p_hr9_vs_hand']:.2f} vs {hand_str}s")
        if pm["p_hr_fb_pct"] >= P_HR_FB_THRESHOLD:
            parts.append(f"Pitcher HR/FB% of {pm['p_hr_fb_pct']:.1f}% is exploitable")
    elif grade == "FLIER":
        if flags_bad:
            parts.append(f"Caution: {', '.join(flags_bad)}")
        if bm["l15_ev"] >= L15_EV_ELITE:
            parts.append(f"Elite contact quality (L15 EV {bm['l15_ev']:.1f})")
        parts.append("Boom-or-bust profile — use sparingly")
    else:
        parts.append("Matchup does not meet HR criteria")
        if flags_bad:
            parts.append(f"Issues: {', '.join(flags_bad)}")

    return ". ".join(parts) + "." if parts else ""

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info(f"Starting PropFinder model for {TODAY}")

    hit_data_map    = load_hit_data()
    splits_map      = load_splits()
    pitcher_map     = load_pitcher_matchup()
    pitch_log_map   = load_pitch_log()

    # Get today's matchup assignments
    rows = query(f"""
        SELECT DISTINCT
            hd.game_pk,
            hd.batter_id,
            hd.batter_name,
            hd.bat_side,
            pm.pitcher_id,
            pm.pitcher_name,
            pm.pitcher_hand,
            pm.opp_team_id,
            COALESCE(pm2.home_team, '') as home_team,
            COALESCE(pm2.away_team, '') as away_team
        FROM {tbl('raw_hit_data')} hd
        JOIN {tbl('raw_pitcher_matchup')} pm
            ON hd.game_pk = pm.game_pk
            AND pm.run_date = '{TODAY}'
            AND pm.split = 'Season'
        LEFT JOIN (
            SELECT DISTINCT game_pk,
                MAX(CASE WHEN pitcher_hand = 'R' THEN pitcher_name END) as home_team,
                MAX(CASE WHEN pitcher_hand = 'L' THEN pitcher_name END) as away_team
            FROM {tbl('raw_pitcher_matchup')}
            WHERE run_date = '{TODAY}'
            GROUP BY game_pk
        ) pm2 ON hd.game_pk = pm2.game_pk
        WHERE hd.run_date = '{TODAY}'
    """)

    if not rows:
        log.warning("No matchup data found for today — did ingest run?")
        return

    output_rows: list[dict] = []
    seen: set = set()

    for r in rows:
        key = (r["batter_id"], r["pitcher_id"], r["game_pk"])
        if key in seen:
            continue
        seen.add(key)

        batter_id   = r["batter_id"]
        pitcher_id  = r["pitcher_id"]
        bat_side    = r.get("bat_side") or "R"
        pitcher_hand = r.get("pitcher_hand") or "R"

        hit_events   = hit_data_map.get(batter_id, [])
        splits       = splits_map.get(batter_id, {})
        p_splits     = pitcher_map.get(pitcher_id, {})
        p_pitch_log  = pitch_log_map.get(pitcher_id, [])

        bm = compute_batter_metrics(
            batter_id, bat_side, pitcher_hand,
            hit_events, splits, p_pitch_log
        )
        pm = compute_pitcher_metrics(
            pitcher_id, bat_side, p_splits, p_pitch_log
        )

        score, flags_good, flags_bad = score_matchup(bm, pm)
        grade = grade_matchup(score, flags_bad)
        why   = build_why(
            r["batter_name"], bat_side,
            r["pitcher_name"], pitcher_hand,
            bm, pm, grade, flags_good, flags_bad
        )

        output_rows.append({
            "run_date":          TODAY.isoformat(),
            "run_timestamp":     NOW.isoformat(),
            "game_pk":           r["game_pk"],
            "home_team":         r.get("home_team", ""),
            "away_team":         r.get("away_team", ""),
            "batter_id":         batter_id,
            "batter_name":       r["batter_name"],
            "bat_side":          bat_side,
            "pitcher_id":        pitcher_id,
            "pitcher_name":      r["pitcher_name"],
            "pitcher_hand":      pitcher_hand,
            **bm,
            **pm,
            "score":             score,
            "grade":             grade,
            "why":               why,
            "flags":             json.dumps(flags_good + [f"⚠ {f}" for f in flags_bad]),
        })

    # Sort by score descending
    output_rows.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"Scored {len(output_rows)} batter/pitcher matchups")

    # Write to BigQuery
    if output_rows:
        errors = bq.insert_rows_json(
            f"{PROJECT}.{DATASET}.hr_picks_daily",
            output_rows
        )
        if errors:
            log.error(f"BQ insert errors: {errors[:3]}")
        else:
            log.info(f"Wrote {len(output_rows)} picks to hr_picks_daily")

    # Print top picks to stdout for GitHub Actions log
    print(f"\n{'='*60}")
    print(f"TOP HR PICKS — {TODAY}")
    print(f"{'='*60}")
    for i, r in enumerate(output_rows[:10], 1):
        if r["grade"] == "SKIP":
            continue
        print(f"{i:2}. {r['batter_name']:<22} vs {r['pitcher_name']:<18} "
              f"[{r['grade']}] score={r['score']:.1f}")

if __name__ == "__main__":
    main()