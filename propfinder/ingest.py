“””
ingest.py — PropFinder data ingestion pipeline
Fetches today’s MLB games, lineups, batter hit-data, splits, and pitcher matchup data.
Writes raw data to BigQuery propfinder dataset.

v3 fixes:

- pitcher splits: 2025 only
- barrel_pct, hard_hit_pct, fb_pct pulled from metricAverages
- HR/FB% computed from homeRuns / airOuts
- batter_team_id stored so model can correctly match batters to pitchers
  “””

import asyncio
import datetime
import logging

import aiohttp
from google.cloud import bigquery

PROJECT  = “graphite-flare-477419-h7”
DATASET  = “propfinder”
BASE_URL = “https://api.propfinder.app”
MLB_API  = “https://statsapi.mlb.com/api/v1”
TODAY    = datetime.date.today()
NOW      = datetime.datetime.now(datetime.timezone.utc)

logging.basicConfig(level=logging.INFO, format=”%(asctime)s %(levelname)s %(message)s”)
log = logging.getLogger(**name**)
bq  = bigquery.Client(project=PROJECT)

def table(name): return f”{PROJECT}.{DATASET}.{name}”

# ── HTTP helper ───────────────────────────────────────────────────────────────

async def get(session, url, params=None):
try:
async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
if r.status == 200:
return await r.json(content_type=None)
log.warning(f”HTTP {r.status} {url} params={params}”)
return None
except Exception as e:
log.error(f”Request failed {url}: {e}”)
return None

def sf(v):
“”“Safe float.”””
try:
s = str(v or “0”)
return float(“0” + s if s.startswith(”.”) else s)
except (ValueError, TypeError):
return None

def si(v):
“”“Safe int.”””
try:
return int(v) if v is not None else None
except (ValueError, TypeError):
return None

def bq_insert(table_name, rows):
if not rows:
log.info(f”No rows for {table_name}”)
return
errors = bq.insert_rows_json(table(table_name), rows)
if errors:
log.error(f”BQ insert errors {table_name}: {errors[:3]}”)
else:
log.info(f”Inserted {len(rows)} rows → {table_name}”)

# ── Step 1: Today’s games ─────────────────────────────────────────────────────

async def fetch_today_games(session):
data = await get(session, f”{MLB_API}/schedule”, params={
“sportId”: 1, “date”: TODAY.isoformat(), “hydrate”: “probablePitcher,team”
})
if not data: return []
games = []
for db in data.get(“dates”, []):
for g in db.get(“games”, []):
home = g[“teams”][“home”]
away = g[“teams”][“away”]
games.append({
“game_pk”:           g[“gamePk”],
“home_team_id”:      home[“team”][“id”],
“away_team_id”:      away[“team”][“id”],
“home_team_name”:    home[“team”][“name”],
“away_team_name”:    away[“team”][“name”],
“home_pitcher_id”:   home.get(“probablePitcher”, {}).get(“id”),
“home_pitcher_name”: home.get(“probablePitcher”, {}).get(“fullName”, “TBD”),
“away_pitcher_id”:   away.get(“probablePitcher”, {}).get(“id”),
“away_pitcher_name”: away.get(“probablePitcher”, {}).get(“fullName”, “TBD”),
})
log.info(f”Found {len(games)} games for {TODAY}”)
return games

# ── Step 2: Lineups ───────────────────────────────────────────────────────────

async def fetch_lineup(session, game_pk):
data = await get(session, f”{BASE_URL}/mlb/lineups”, params={“gameIds”: game_pk})
if not data or not isinstance(data, list):
return {“home_players”: [], “away_players”: []}
for item in data:
if item.get(“gamePk”) == game_pk:
lu = item.get(“lineups”, {})
return {
“home_players”: [p[“id”] for p in lu.get(“homePlayers”, [])],
“away_players”: [p[“id”] for p in lu.get(“awayPlayers”, [])],
}
return {“home_players”: [], “away_players”: []}

# ── Step 3: Batter hit-data ───────────────────────────────────────────────────

async def fetch_hit_data(session, batter_id, game_pk, batter_team_id):
data = await get(session, f”{BASE_URL}/mlb/hit-data”,
params={“playerId”: batter_id, “group”: “hitting”})
if not data or not isinstance(data, list): return []
rows = []
for ev in data:
ls = sf(ev.get(“launchSpeed”))
if ls is None: continue
rows.append({
“run_date”:        TODAY.isoformat(),
“game_pk”:         game_pk,
“batter_id”:       batter_id,
“batter_team_id”:  batter_team_id,
“batter_name”:     ev.get(“batterName”, “”),
“bat_side”:        ev.get(“batSide”, “”),
“pitcher_id”:      si(ev.get(“pitcherId”)),
“pitcher_name”:    ev.get(“pitcherName”, “”),
“pitch_hand”:      ev.get(“pitchHand”, “”),
“pitch_type”:      ev.get(“pitchType”, “”),
“result”:          ev.get(“result”, “”),
“launch_speed”:    ls,
“launch_angle”:    sf(ev.get(“launchAngle”)),
“total_distance”:  sf(ev.get(“totalDistance”)),
“trajectory”:      ev.get(“trajectory”, “”),
“is_barrel”:       bool(ev.get(“isBarrel”)),
“hr_in_n_parks”:   si(ev.get(“hrInNParks”, 0)),
“event_date”:      ev.get(“date”, “”)[:10],
“season”:          si(ev.get(“season”)),
“ingested_at”:     NOW.isoformat(),
})
return rows

# ── Step 4: Batter splits ─────────────────────────────────────────────────────

SPLITS_WANTED = {“vl”, “vr”, “h”, “a”, “r”, “g”, “t”, “preas”, “posas”}

async def fetch_splits(session, batter_id, batter_name):
data = await get(session, f”{BASE_URL}/mlb/splits”,
params={“playerId”: batter_id, “group”: “hitting”})
if not data or not isinstance(data, list): return []
rows = []
for s in data:
code = s.get(“splitCode”, “”)
if code not in SPLITS_WANTED: continue
stat = s.get(“stat”, {})
ab   = si(stat.get(“atBats”, 0)) or 0
hr   = si(stat.get(“homeRuns”, 0)) or 0
dbl  = si(stat.get(“doubles”, 0)) or 0
tri  = si(stat.get(“triples”, 0)) or 0

```
    def ps(v):
        try:
            sv = str(v or "0")
            return float("0" + sv if sv.startswith(".") else sv)
        except: return 0.0

    rows.append({
        "run_date":    TODAY.isoformat(),
        "batter_id":   batter_id,
        "batter_name": batter_name,
        "split_code":  code,
        "split_name":  s.get("splitName", ""),
        "season":      str(s.get("season", "")),
        "avg":         ps(stat.get("avg")),
        "obp":         ps(stat.get("obp")),
        "slg":         ps(stat.get("slg")),
        "ops":         ps(stat.get("ops")),
        "home_runs":   hr,
        "at_bats":     ab,
        "hits":        si(stat.get("hits", 0)),
        "doubles":     dbl,
        "triples":     tri,
        "strike_outs": si(stat.get("strikeOuts", 0)),
        "ingested_at": NOW.isoformat(),
    })
return rows
```

# ── Step 5: Pitcher matchup ───────────────────────────────────────────────────

async def fetch_pitcher_matchup(session, pitcher_id, opp_team_id, game_pk, pitcher_name):
data = await get(session, f”{BASE_URL}/mlb/pitcher-matchup”, params={
“pitcherId”: pitcher_id, “opposingTeamId”: opp_team_id,
})
if not data or not isinstance(data, dict):
return {“splits”: [], “pitch_log”: []}

```
pitcher_hand = data.get("pitchingType", "")

# Build metricAverages lookup by split: vsLHB / vsRHB / Season
metric_map = {}
for m in data.get("metricAverages", []):
    split_key = m.get("split", "Season")
    metric_map[split_key] = m

split_rows = []
for s in data.get("splits", []):
    # 2025 only
    if si(s.get("season")) != 2025:
        continue

    split_key = s.get("split", "Season")
    metrics   = metric_map.get(split_key, metric_map.get("Season", {}))

    # Barrel% and HardHit% from metricAverages (stored as decimals 0-1)
    raw_barrel = metrics.get("seasonBarrel") or 0
    barrel_pct = float(raw_barrel) * 100 if float(raw_barrel or 0) <= 1 else float(raw_barrel)

    raw_hh = metrics.get("seasonHardHitPercentage") or 0
    hard_hit_pct = float(raw_hh) * 100 if float(raw_hh or 0) <= 1 else float(raw_hh)

    # FB% from metricAverages flyballPercentage (decimal 0-1)
    raw_fb = metrics.get("flyballPercentage") or 0
    fb_pct = float(raw_fb) * 100 if float(raw_fb or 0) <= 1 else float(raw_fb)

    # HR/FB% — computed directly: homeRuns / airOuts (airOuts ≈ fly balls + line drives)
    hr_n    = si(s.get("homeRuns")) or 0
    air_outs = si(s.get("airOuts")) or 0
    hr_fb_pct = round((hr_n / air_outs) * 100, 2) if air_outs > 0 else 0.0

    split_rows.append({
        "run_date":      TODAY.isoformat(),
        "game_pk":       game_pk,
        "pitcher_id":    pitcher_id,
        "pitcher_name":  pitcher_name,
        "pitcher_hand":  pitcher_hand,
        "opp_team_id":   opp_team_id,
        "split":         split_key,
        "ip":            sf(s.get("ip")),
        "home_runs":     hr_n,
        "hr_per_9":      sf(s.get("homeRunsPer9Inn")),
        "barrel_pct":    round(barrel_pct, 4),
        "hard_hit_pct":  round(hard_hit_pct, 4),
        "fb_pct":        round(fb_pct, 4),
        "hr_fb_pct":     hr_fb_pct,
        "whip":          sf(s.get("whip")),
        "woba":          sf(s.get("woba")),
        "ingested_at":   NOW.isoformat(),
    })

# Pitch log — 2025 only
pitch_log_rows = []
for p in data.get("pitchLog", []):
    if si(p.get("season")) != 2025:
        continue
    pitch_log_rows.append({
        "run_date":    TODAY.isoformat(),
        "game_pk":     game_pk,
        "pitcher_id":  pitcher_id,
        "batter_hand": p.get("type", ""),
        "pitch_code":  p.get("pitchCode", ""),
        "pitch_name":  p.get("pitchName", ""),
        "season":      2025,
        "count":       si(p.get("count")),
        "percentage":  sf(p.get("percentage")),
        "home_runs":   si(p.get("homeRuns")),
        "woba":        sf(p.get("wOBA")),
        "slg":         sf(p.get("slg")),
        "iso":         sf(p.get("iso")),
        "whiff":       sf(p.get("whiff")),
        "k_percent":   sf(p.get("kPercent")),
        "ingested_at": NOW.isoformat(),
    })

return {"splits": split_rows, "pitch_log": pitch_log_rows}
```

# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
log.info(f”Starting PropFinder ingest for {TODAY}”)

```
connector = aiohttp.TCPConnector(limit=20)
async with aiohttp.ClientSession(connector=connector) as session:

    # 1. Games
    games = await fetch_today_games(session)
    if not games:
        log.warning("No games found — exiting")
        return

    # 2. Lineups
    lineup_results = await asyncio.gather(*[fetch_lineup(session, g["game_pk"]) for g in games])
    lineups = {g["game_pk"]: r for g, r in zip(games, lineup_results)}

    # 3. Build batter + pitcher job lists
    batter_jobs  = []
    pitcher_jobs = []

    for g in games:
        gp = g["game_pk"]
        lu = lineups.get(gp, {"home_players": [], "away_players": []})

        # Home batters face away pitcher
        for bid in lu["home_players"]:
            batter_jobs.append({
                "batter_id":      bid,
                "game_pk":        gp,
                "batter_team_id": g["home_team_id"],
            })
        # Away batters face home pitcher
        for bid in lu["away_players"]:
            batter_jobs.append({
                "batter_id":      bid,
                "game_pk":        gp,
                "batter_team_id": g["away_team_id"],
            })

        if g["home_pitcher_id"]:
            pitcher_jobs.append({
                "pitcher_id":   g["home_pitcher_id"],
                "pitcher_name": g["home_pitcher_name"],
                "opp_team_id":  g["away_team_id"],
                "game_pk":      gp,
            })
        if g["away_pitcher_id"]:
            pitcher_jobs.append({
                "pitcher_id":   g["away_pitcher_id"],
                "pitcher_name": g["away_pitcher_name"],
                "opp_team_id":  g["home_team_id"],
                "game_pk":      gp,
            })

    log.info(f"Batter jobs: {len(batter_jobs)} | Pitcher jobs: {len(pitcher_jobs)}")

    # 4. Fetch batter data
    all_hit_rows   = []
    all_split_rows = []

    async def fetch_batter(job):
        hit_rows   = await fetch_hit_data(session, job["batter_id"], job["game_pk"], job["batter_team_id"])
        bname      = hit_rows[0]["batter_name"] if hit_rows else str(job["batter_id"])
        split_rows = await fetch_splits(session, job["batter_id"], bname)
        return hit_rows, split_rows

    for hit_rows, split_rows in await asyncio.gather(*[fetch_batter(j) for j in batter_jobs]):
        all_hit_rows.extend(hit_rows)
        all_split_rows.extend(split_rows)

    # 5. Fetch pitcher data
    all_pitcher_rows   = []
    all_pitch_log_rows = []

    for result in await asyncio.gather(*[
        fetch_pitcher_matchup(session, j["pitcher_id"], j["opp_team_id"], j["game_pk"], j["pitcher_name"])
        for j in pitcher_jobs
    ]):
        all_pitcher_rows.extend(result["splits"])
        all_pitch_log_rows.extend(result["pitch_log"])

    # 6. Write to BQ
    bq_insert("raw_hit_data",        all_hit_rows)
    bq_insert("raw_splits",          all_split_rows)
    bq_insert("raw_pitcher_matchup", all_pitcher_rows)
    bq_insert("raw_pitch_log",       all_pitch_log_rows)

    log.info("Ingest complete.")
```

if **name** == “**main**”:
asyncio.run(main())