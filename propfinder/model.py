“””
model.py — PropFinder HR Pulse Score
Scale: 0-100. Labels: Ideal (75+) | Favorable (55-74) | Average (35-54) | Avoid (<35)

Fixes v2:

- pitcher_hand normalized from LHP/RHP to L/R for hit_data filter
- pitch log deduped by pitch name (keep highest pct), 2025 only
- batter matched to their actual pitcher via batter_team_id = opp_team_id
- splits lookup uses normalized hand char
  “””

import datetime
import json
import logging
from collections import defaultdict
from statistics import mean

from google.cloud import bigquery

PROJECT = “graphite-flare-477419-h7”
DATASET = “propfinder”
TODAY   = datetime.date.today()
NOW     = datetime.datetime.now(datetime.timezone.utc)

logging.basicConfig(level=logging.INFO, format=”%(asctime)s %(levelname)s %(message)s”)
log = logging.getLogger(**name**)
bq  = bigquery.Client(project=PROJECT)

def tbl(name): return f”`{PROJECT}.{DATASET}.{name}`”

# ── Thresholds ────────────────────────────────────────────────────────────────

P_HR9_IDEAL=1.8;   P_HR9_FAV=1.5;   P_HR9_AVG=1.2;   P_HR9_AVOID=1.0
P_HRFB_IDEAL=20.0; P_HRFB_FAV=15.0; P_HRFB_AVG=10.0
P_FB_IDEAL=40.0;   P_FB_FAV=35.0
P_BARREL_ELITE=10.0; P_HARDHIT_ATTACK=40.0; P_ISO_EXPLOIT=0.200

B_ISO_ELITE=0.300; B_ISO_FAV=0.200; B_ISO_AVG=0.150
B_SLG_ELITE=0.500; B_SLG_FAV=0.450; B_SLG_AVG=0.400
B_EV_ELITE=92.0;   B_EV_FAV=89.0;   B_EV_AVG=85.0
B_BAR_ELITE=20.0;  B_BAR_FAV=12.0;  B_BAR_AVG=7.0

PULSE_IDEAL=75; PULSE_FAV=55; PULSE_AVG=35
RAW_MAX = 125.0

# ── Helpers ───────────────────────────────────────────────────────────────────

def query(sql):
return [dict(r) for r in bq.query(sql).result()]

def safe_float(v, default=0.0):
try:
s = str(v or “0”)
return float(“0” + s if s.startswith(”.”) else s)
except (ValueError, TypeError):
return default

def tier(val, elite, fav, avg):
if val is None: return “below”
if val >= elite: return “elite”
if val >= fav:   return “favorable”
if val >= avg:   return “average”
return “below”

def hc(pitcher_hand):
“”“Normalize LHP/RHP or L/R to single char.”””
return “L” if str(pitcher_hand or “”).upper() in (“L”, “LHP”) else “R”

# ── Load BQ data ──────────────────────────────────────────────────────────────

def load_hit_data():
rows = query(f”SELECT * FROM {tbl(‘raw_hit_data’)} WHERE run_date=’{TODAY}’ ORDER BY batter_id, event_date DESC”)
out = defaultdict(list)
for r in rows: out[r[“batter_id”]].append(r)
return out

def load_splits():
rows = query(f”SELECT * FROM {tbl(‘raw_splits’)} WHERE run_date=’{TODAY}’”)
out = defaultdict(dict)
for r in rows: out[r[“batter_id”]][r[“split_code”]] = r
return out

def load_pitcher_matchup():
rows = query(f”SELECT * FROM {tbl(‘raw_pitcher_matchup’)} WHERE run_date=’{TODAY}’”)
out = defaultdict(dict)
for r in rows: out[r[“pitcher_id”]][r[“split”]] = r
return out

def load_pitch_log():
rows = query(f”SELECT * FROM {tbl(‘raw_pitch_log’)} WHERE run_date=’{TODAY}’ AND season=2025 ORDER BY pitcher_id, percentage DESC”)
out = defaultdict(list)
for r in rows: out[r[“pitcher_id”]].append(r)
return out

def load_today_matchups():
“””
Join on batter_team_id = opp_team_id so each batter only faces
the pitcher whose team they are NOT on.
“””
return query(f”””
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
FROM {tbl(‘raw_hit_data’)} hd
JOIN {tbl(‘raw_pitcher_matchup’)} pm
ON  hd.game_pk        = pm.game_pk
AND pm.run_date       = ‘{TODAY}’
AND pm.split          = ‘Season’
AND hd.batter_team_id = pm.opp_team_id
WHERE hd.run_date = ‘{TODAY}’
“””)

def load_game_names():
“”“Returns {game_pk: {home_team_name, away_team_name}} from pitcher matchup.”””
rows = query(f”””
SELECT DISTINCT game_pk, pitcher_name, pitcher_hand, opp_team_id
FROM {tbl(‘raw_pitcher_matchup’)}
WHERE run_date=’{TODAY}’ AND split=‘Season’
“””)
# We’ll derive names from pitcher data — home pitcher faces away team
# For display we just use pitcher names as team proxies for now
games = {}
for r in rows:
gp = r[“game_pk”]
if gp not in games:
games[gp] = {“pitchers”: []}
games[gp][“pitchers”].append(r[“pitcher_name”])
return games

# ── Batter metrics ────────────────────────────────────────────────────────────

def compute_batter_metrics(batter_id, bat_side, pitcher_hand_raw, hit_events, splits, pitch_log):
hand_char   = hc(pitcher_hand_raw)
hand_code   = “LHB” if bat_side == “L” else “RHB”

```
# Dedupe pitch log: keep highest pct per pitch name, 2025 only
seen: dict = {}
for p in pitch_log:
    if p["batter_hand"] != hand_code: continue
    name = p["pitch_name"]
    pct  = p.get("percentage") or 0
    if name not in seen or pct > seen[name]:
        seen[name] = pct
primary = {name for name, pct in seen.items() if pct > 0.05}

# Filter hit events: pitcher handedness + primary pitch types
filtered = [
    e for e in hit_events
    if e["pitch_hand"] == hand_char
    and (not primary or e["pitch_type"] in primary)
]

l15 = filtered[:15]
n   = len(l15)
l15_barrel = sum(1 for e in l15 if e.get("is_barrel")) / n * 100 if n else 0.0
l15_ev     = mean(e["launch_speed"] for e in l15) if n else 0.0
l15_hh     = sum(1 for e in l15 if (e.get("launch_speed") or 0) >= 95) / n * 100 if n else 0.0

log.debug(f"  Batter {batter_id}: {len(filtered)} filtered events, {n} in L15, pitches={primary}")

# Season 2025 (no pitch filter)
s25    = [e for e in hit_events if e.get("season") == 2025]
n25    = len(s25)
s25_barrel = sum(1 for e in s25 if e.get("is_barrel")) / n25 * 100 if n25 else 0.0
s25_ev     = mean(e["launch_speed"] for e in s25) if n25 else 0.0

# HR/FB%
fb    = [e for e in hit_events if e.get("trajectory") == "fly_ball"]
hr_fb = sum(1 for e in fb if e["result"] == "home_run") / len(fb) * 100 if fb else 0.0

# ISO + SLG from splits vs pitcher hand
sp  = splits.get("vl" if hand_char == "L" else "vr", {})
ab  = int(sp.get("at_bats") or 0)
hr  = int(sp.get("home_runs") or 0)
dbl = int(sp.get("doubles") or 0)
tri = int(sp.get("triples") or 0)
iso = (dbl + 2*tri + 3*hr) / ab if ab > 0 else 0.0
slg = safe_float(sp.get("slg"))

return {
    "iso":               round(iso, 3),
    "slg":               round(slg, 3),
    "l15_ev":            round(l15_ev, 1),
    "l15_barrel_pct":    round(l15_barrel, 1),
    "season_ev":         round(s25_ev, 1),
    "season_barrel_pct": round(s25_barrel, 1),
    "l15_hard_hit_pct":  round(l15_hh, 1),
    "hr_fb_pct":         round(hr_fb, 1),
}
```

# ── Pitcher metrics ───────────────────────────────────────────────────────────

def compute_pitcher_metrics(pitcher_id, bat_side, matchup_splits):
hs = matchup_splits.get(“vsLHB” if bat_side == “L” else “vsRHB”, {})
ss = matchup_splits.get(“Season”, {})

```
p_hr9   = safe_float(hs.get("hr_per_9"))
p_hr9_s = safe_float(ss.get("hr_per_9"))
p_hr_n  = int(hs.get("home_runs") or 0)
p_ip    = safe_float(hs.get("ip"))

raw_bar  = hs.get("barrel_pct") or ss.get("barrel_pct") or 0
p_barrel = safe_float(raw_bar) * 100 if safe_float(raw_bar) < 1 else safe_float(raw_bar)

raw_hh = ss.get("hard_hit_pct") or 0
p_hh   = safe_float(raw_hh) * 100 if safe_float(raw_hh) < 1 else safe_float(raw_hh)

raw_fb = hs.get("fb_pct") or ss.get("fb_pct") or 0
p_fb   = safe_float(raw_fb) * 100 if safe_float(raw_fb) < 1 else safe_float(raw_fb)

# HR/FB% — use stored value from ingest first, fall back to estimate
stored_hrfb = safe_float(hs.get("hr_fb_pct"))
if stored_hrfb and stored_hrfb > 0:
    p_hrfb = stored_hrfb
elif p_ip > 0 and p_fb > 0:
    est_fb = p_ip * (p_fb / 100) * 1.2
    p_hrfb = min((p_hr_n / est_fb) * 100, 60.0) if est_fb > 0 else 0.0
else:
    p_hrfb = 0.0

woba  = safe_float(hs.get("woba"))
p_iso = 0.230 if woba >= 0.340 else (0.175 if woba >= 0.310 else 0.125)

return {
    "p_hr9_season":   round(p_hr9_s, 2),
    "p_hr9_vs_hand":  round(p_hr9, 2),
    "p_barrel_pct":   round(p_barrel, 1),
    "p_hr_fb_pct":    round(p_hrfb, 1),
    "p_hr_vs_hand":   p_hr_n,
    "p_fb_pct":       round(p_fb, 1),
    "p_hard_hit_pct": round(p_hh, 1),
    "p_iso_allowed":  round(p_iso, 3),
}
```

# ── Pulse Score ───────────────────────────────────────────────────────────────

def compute_pulse_score(bm, pm, bat_side, pitcher_hand_raw):
raw = 0.0
flags_good, flags_bad = [], []
hand_char_val = hc(pitcher_hand_raw)
hb = “LHB” if bat_side == “L” else “RHB”
hp = “LHP” if hand_char_val == “L” else “RHP”

```
hr9  = pm["p_hr9_vs_hand"]
hrfb = pm["p_hr_fb_pct"]

# PITCHER (max 65 pts)
if   hr9 >= P_HR9_IDEAL: raw += 20; flags_good.append(f"HR/9 vs {hb}: {hr9:.2f} 🎯")
elif hr9 >= P_HR9_FAV:   raw += 15; flags_good.append(f"HR/9 vs {hb}: {hr9:.2f} ✅")
elif hr9 >= P_HR9_AVG:   raw += 8;  flags_good.append(f"HR/9 vs {hb}: {hr9:.2f} ☑️")
elif hr9 < P_HR9_AVOID:  raw -= 5;  flags_bad.append(f"HR/9 vs {hb}: {hr9:.2f} ❌")

if   hrfb >= P_HRFB_IDEAL: raw += 18; flags_good.append(f"HR/FB% vs {hb}: {hrfb:.1f}% 🎯")
elif hrfb >= P_HRFB_FAV:   raw += 12; flags_good.append(f"HR/FB% vs {hb}: {hrfb:.1f}% ✅")
elif hrfb >= P_HRFB_AVG:   raw += 5;  flags_good.append(f"HR/FB% vs {hb}: {hrfb:.1f}% ☑️")
else:                       raw -= 3;  flags_bad.append(f"HR/FB% vs {hb}: {hrfb:.1f}% ❌")

fb = pm["p_fb_pct"]
if   fb > P_FB_IDEAL: raw += 8; flags_good.append(f"FB%: {fb:.1f}% 🎯")
elif fb >= P_FB_FAV:  raw += 4; flags_good.append(f"FB%: {fb:.1f}% ✅")

if   pm["p_barrel_pct"] > P_BARREL_ELITE: raw += 7; flags_good.append(f"Barrel% allowed: {pm['p_barrel_pct']:.1f}% 🎯")
elif pm["p_barrel_pct"] >= 7:              raw += 3; flags_good.append(f"Barrel% allowed: {pm['p_barrel_pct']:.1f}% ✅")

if   pm["p_hard_hit_pct"] >= P_HARDHIT_ATTACK: raw += 5; flags_good.append(f"HardHit% allowed: {pm['p_hard_hit_pct']:.1f}% ✅")
elif pm["p_hard_hit_pct"] >= 35:                raw += 2

if   pm["p_iso_allowed"] >= P_ISO_EXPLOIT: raw += 4; flags_good.append(f"ISO allowed: {pm['p_iso_allowed']:.3f} ✅")
elif pm["p_iso_allowed"] >= 0.160:         raw += 2

if bat_side == hand_char_val:
    raw += 3; flags_good.append(f"{hb} platoon edge ✅")

# BATTER (max 60 pts)
iso_t = tier(bm["iso"],            B_ISO_ELITE, B_ISO_FAV, B_ISO_AVG)
slg_t = tier(bm["slg"],            B_SLG_ELITE, B_SLG_FAV, B_SLG_AVG)
ev_t  = tier(bm["l15_ev"],         B_EV_ELITE,  B_EV_FAV,  B_EV_AVG)
bar_t = tier(bm["l15_barrel_pct"], B_BAR_ELITE, B_BAR_FAV, B_BAR_AVG)

pts = {"elite": 15, "favorable": 10, "average": 5, "below": 0}
raw += pts[iso_t] + pts[slg_t] + pts[ev_t] + pts[bar_t]
if iso_t == "below": raw -= 2; flags_bad.append(f"ISO {bm['iso']:.3f} (weak)")

em = {"elite": "🎯", "favorable": "✅", "average": "☑️", "below": ""}
if iso_t != "below": flags_good.append(f"ISO {bm['iso']:.3f} {em[iso_t]}")
if slg_t != "below": flags_good.append(f"SLG {bm['slg']:.3f} {em[slg_t]}")
if ev_t  != "below": flags_good.append(f"L15 EV {bm['l15_ev']} mph {em[ev_t]}")
if bar_t != "below": flags_good.append(f"L15 Barrel {bm['l15_barrel_pct']:.1f}% {em[bar_t]}")

if   bm["season_barrel_pct"] >= B_BAR_ELITE: flags_good.append(f"'25 Barrel {bm['season_barrel_pct']:.1f}% 🎯")
elif bm["season_barrel_pct"] >= B_BAR_FAV:   flags_good.append(f"'25 Barrel {bm['season_barrel_pct']:.1f}% ✅")

pulse = round(max(0.0, min(100.0, (raw / RAW_MAX) * 100)), 1)

pitcher_avoid = hr9 < P_HR9_AVOID and hrfb < P_HRFB_AVG
if   pitcher_avoid or pulse < PULSE_AVG: label = "AVOID"
elif pulse >= PULSE_IDEAL:               label = "IDEAL"
elif pulse >= PULSE_FAV:                 label = "FAVORABLE"
else:                                    label = "AVERAGE"

return pulse, label, flags_good, flags_bad
```

# ── Why text ──────────────────────────────────────────────────────────────────

def build_why(batter_name, bat_side, pitcher_name, pitcher_hand_raw, bm, pm, label, flags_good, flags_bad):
hb = “LHB” if bat_side == “L” else “RHB”
hp = “LHP” if hc(pitcher_hand_raw) == “L” else “RHP”
parts = []
if label == “IDEAL”:
if pm[“p_hr9_vs_hand”] >= P_HR9_FAV:
parts.append(f”{hp} {pitcher_name} highly exploitable vs {hb}s (HR/9: {pm[‘p_hr9_vs_hand’]:.2f})”)
if pm[“p_hr_fb_pct”] >= P_HRFB_FAV:
parts.append(f”HR/FB% of {pm[‘p_hr_fb_pct’]:.1f}% reveals power vulnerability”)
if bm[“l15_barrel_pct”] >= B_BAR_ELITE:
parts.append(f”Elite L15 Barrel% of {bm[‘l15_barrel_pct’]:.1f}%”)
if bm[“iso”] >= B_ISO_ELITE:
parts.append(f”Elite ISO {bm[‘iso’]:.3f} vs {hp}s”)
elif label == “FAVORABLE”:
if pm[“p_hr9_vs_hand”] >= P_HR9_AVG:
parts.append(f”{pitcher_name} ({hp}) HR/9 vs {hb}: {pm[‘p_hr9_vs_hand’]:.2f}”)
if bm[“l15_barrel_pct”] >= B_BAR_FAV:
parts.append(f”Strong Barrel% trend ({bm[‘l15_barrel_pct’]:.1f}%)”)
if bm[“iso”] >= B_ISO_FAV:
parts.append(f”Favorable ISO {bm[‘iso’]:.3f}”)
if bm[“l15_ev”] >= B_EV_FAV:
parts.append(f”L15 EV {bm[‘l15_ev’]:.1f} mph”)
elif label == “AVERAGE”:
parts.append(“Some signals present but not fully confirmed”)
if bm[“l15_ev”] >= B_EV_ELITE:
parts.append(f”Elite contact quality (L15 EV {bm[‘l15_ev’]:.1f}) — worth watching”)
parts.append(“Use sparingly”)
else:
parts.append(“Pitcher not exploitable vs this handedness”)
if flags_bad:
parts.append(f”Issues: {’, ‘.join(f.replace(’ ❌’,’’) for f in flags_bad)}”)
return “. “.join(parts) + “.” if parts else “”

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
log.info(f”Starting PropFinder Pulse Score model for {TODAY}”)

```
hit_data_map  = load_hit_data()
splits_map    = load_splits()
pitcher_map   = load_pitcher_matchup()
pitch_log_map = load_pitch_log()
matchups      = load_today_matchups()

if not matchups:
    log.warning("No matchup data — did ingest run with the updated ingest.py?")
    return

log.info(f"Processing {len(matchups)} batter/pitcher matchups")

output_rows = []
seen = set()

for r in matchups:
    key = (r["batter_id"], r["pitcher_id"], r["game_pk"])
    if key in seen: continue
    seen.add(key)

    hit_events = hit_data_map.get(r["batter_id"], [])
    if not hit_events: continue

    bm = compute_batter_metrics(
        r["batter_id"], r["bat_side"], r["pitcher_hand"],
        hit_events, splits_map.get(r["batter_id"], {}),
        pitch_log_map.get(r["pitcher_id"], [])
    )
    pm = compute_pitcher_metrics(
        r["pitcher_id"], r["bat_side"],
        pitcher_map.get(r["pitcher_id"], {})
    )
    pulse, label, flags_good, flags_bad = compute_pulse_score(
        bm, pm, r["bat_side"], r["pitcher_hand"]
    )
    why = build_why(
        r["batter_name"], r["bat_side"],
        r["pitcher_name"], r["pitcher_hand"],
        bm, pm, label, flags_good, flags_bad
    )

    output_rows.append({
        "run_date":       TODAY.isoformat(),
        "run_timestamp":  NOW.isoformat(),
        "game_pk":        r["game_pk"],
        "home_team":      "",
        "away_team":      "",
        "batter_id":      r["batter_id"],
        "batter_name":    r["batter_name"],
        "bat_side":       r["bat_side"],
        "pitcher_id":     r["pitcher_id"],
        "pitcher_name":   r["pitcher_name"],
        "pitcher_hand":   r["pitcher_hand"],
        **bm,
        **pm,
        "score":          pulse,
        "grade":          label,
        "why":            why,
        "flags":          json.dumps(flags_good + flags_bad),
    })

output_rows.sort(key=lambda x: x["score"], reverse=True)
log.info(f"Scored {len(output_rows)} matchups")

if output_rows:
    errors = bq.insert_rows_json(f"{PROJECT}.{DATASET}.hr_picks_daily", output_rows)
    if errors:
        log.error(f"BQ insert errors: {errors[:3]}")
    else:
        log.info(f"Wrote {len(output_rows)} picks to hr_picks_daily")

print(f"\n{'='*70}")
print(f"PULSE SCORE — HR PICKS — {TODAY}")
print(f"{'='*70}")
for label in ["IDEAL", "FAVORABLE", "AVERAGE", "AVOID"]:
    group = [r for r in output_rows if r["grade"] == label]
    if not group: continue
    print(f"\n── {label} ──")
    for r in group:
        hb = "LHB" if r["bat_side"] == "L" else "RHB"
        hp = "LHP" if hc(r["pitcher_hand"]) == "L" else "RHP"
        print(f"  {r['batter_name']:<22} ({hb}) vs {r['pitcher_name']:<18} ({hp})  Pulse: {r['score']}")
        print(f"    ISO:{r['iso']:.3f}  SLG:{r['slg']:.3f}  "
              f"L15EV:{r['l15_ev']}  L15Barrel:{r['l15_barrel_pct']:.1f}%  "
              f"| P HR/9:{r['p_hr9_vs_hand']:.2f}  HR/FB:{r['p_hr_fb_pct']:.1f}%")
        if r["why"]:
            print(f"    {r['why']}")
skipped = sum(1 for r in output_rows if r["grade"] == "AVOID")
print(f"\n── AVOID: {skipped} matchups did not meet criteria ──")
```

if **name** == “**main**”:
main()