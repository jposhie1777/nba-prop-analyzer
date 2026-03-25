"""
ingest.py — PropFinder data ingestion pipeline
Fetches today's MLB games, lineups, batter hit-data, splits, and pitcher matchup data.
Writes raw data to BigQuery propfinder dataset.
"""

import asyncio
import datetime
import logging
from typing import Optional

import aiohttp
from google.cloud import bigquery

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT  = "graphite-flare-477419-h7"
DATASET  = "propfinder"
BASE_URL = "https://api.propfinder.app"
MLB_API  = "https://statsapi.mlb.com/api/v1"
TODAY    = datetime.date.today()
NOW      = datetime.datetime.now(datetime.timezone.utc)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
bq  = bigquery.Client(project=PROJECT)

def table(name): return f"{PROJECT}.{DATASET}.{name}"

# ── HTTP helper ───────────────────────────────────────────────────────────────
async def get(session, url, params=None):
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            log.warning(f"HTTP {r.status} {url} params={params}")
            return None
    except Exception as e:
        log.error(f"Request failed {url}: {e}")
        return None

def safe_float(v):
    try:
        s = str(v or "0")
        return float("0" + s if s.startswith(".") else s)
    except (ValueError, TypeError):
        return None

def safe_int(v):
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None

def bq_insert(table_name, rows):
    if not rows:
        log.info(f"No rows to insert into {table_name}")
        return
    errors = bq.insert_rows_json(table(table_name), rows)
    if errors:
        log.error(f"BQ insert errors for {table_name}: {errors[:3]}")
    else:
        log.info(f"Inserted {len(rows)} rows into {table_name}")

# ── Step 1: Today's games + probable pitchers ─────────────────────────────────
async def fetch_today_games(session):
    data = await get(session, f"{MLB_API}/schedule", params={
        "sportId": 1,
        "date": TODAY.isoformat(),
        "hydrate": "probablePitcher,team"
    })
    if not data:
        return []

    games = []
    for date_block in data.get("dates", []):
        for g in date_block.get("games", []):
            home = g["teams"]["home"]
            away = g["teams"]["away"]
            games.append({
                "game_pk":           g["gamePk"],
                "home_team_id":      home["team"]["id"],
                "away_team_id":      away["team"]["id"],
                "home_team_name":    home["team"]["name"],
                "away_team_name":    away["team"]["name"],
                "home_pitcher_id":   home.get("probablePitcher", {}).get("id"),
                "home_pitcher_name": home.get("probablePitcher", {}).get("fullName", "TBD"),
                "away_pitcher_id":   away.get("probablePitcher", {}).get("id"),
                "away_pitcher_name": away.get("probablePitcher", {}).get("fullName", "TBD"),
            })
    log.info(f"Found {len(games)} games for {TODAY}")
    return games

# ── Step 2: Lineups ───────────────────────────────────────────────────────────
async def fetch_lineup(session, game_pk):
    data = await get(session, f"{BASE_URL}/mlb/lineups", params={"gameIds": game_pk})
    if not data or not isinstance(data, list):
        return {"home_players": [], "away_players": []}
    for item in data:
        if item.get("gamePk") == game_pk:
            lineups = item.get("lineups", {})
            return {
                "home_players": [p["id"] for p in lineups.get("homePlayers", [])],
                "away_players": [p["id"] for p in lineups.get("awayPlayers", [])],
            }
    return {"home_players": [], "away_players": []}

# ── Step 3: Batter hit-data ───────────────────────────────────────────────────
async def fetch_hit_data(session, batter_id, game_pk, batter_team_id):
    data = await get(session, f"{BASE_URL}/mlb/hit-data",
                     params={"playerId": batter_id, "group": "hitting"})
    if not data or not isinstance(data, list):
        return []

    rows = []
    for ev in data:
        launch_speed = safe_float(ev.get("launchSpeed"))
        if launch_speed is None:
            continue
        rows.append({
            "run_date":        TODAY.isoformat(),
            "game_pk":         game_pk,
            "batter_id":       batter_id,
            "batter_team_id":  batter_team_id,
            "batter_name":     ev.get("batterName", ""),
            "bat_side":        ev.get("batSide", ""),
            "pitcher_id":      safe_int(ev.get("pitcherId")),
            "pitcher_name":    ev.get("pitcherName", ""),
            "pitch_hand":      ev.get("pitchHand", ""),
            "pitch_type":      ev.get("pitchType", ""),
            "result":          ev.get("result", ""),
            "launch_speed":    launch_speed,
            "launch_angle":    safe_float(ev.get("launchAngle")),
            "total_distance":  safe_float(ev.get("totalDistance")),
            "trajectory":      ev.get("trajectory", ""),
            "is_barrel":       bool(ev.get("isBarrel")),
            "hr_in_n_parks":   safe_int(ev.get("hrInNParks", 0)),
            "event_date":      ev.get("date", "")[:10],
            "season":          safe_int(ev.get("season")),
            "ingested_at":     NOW.isoformat(),
        })
    return rows

# ── Step 4: Batter splits ─────────────────────────────────────────────────────
SPLITS_WANTED = {"vl", "vr", "h", "a", "r", "g", "t", "preas", "posas"}

async def fetch_splits(session, batter_id, batter_name):
    data = await get(session, f"{BASE_URL}/mlb/splits",
                     params={"playerId": batter_id, "group": "hitting"})
    if not data or not isinstance(data, list):
        return []

    rows = []
    for s in data:
        code = s.get("splitCode", "")
        if code not in SPLITS_WANTED:
            continue
        stat = s.get("stat", {})
        ab   = safe_int(stat.get("atBats", 0)) or 0
        hr   = safe_int(stat.get("homeRuns", 0)) or 0
        dbl  = safe_int(stat.get("doubles", 0)) or 0
        tri  = safe_int(stat.get("triples", 0)) or 0

        def parse_stat(v):
            try:
                sv = str(v or "0")
                return float("0" + sv if sv.startswith(".") else sv)
            except (ValueError, TypeError):
                return 0.0

        rows.append({
            "run_date":    TODAY.isoformat(),
            "batter_id":   batter_id,
            "batter_name": batter_name,
            "split_code":  code,
            "split_name":  s.get("splitName", ""),
            "season":      str(s.get("season", "")),
            "avg":         parse_stat(stat.get("avg")),
            "obp":         parse_stat(stat.get("obp")),
            "slg":         parse_stat(stat.get("slg")),
            "ops":         parse_stat(stat.get("ops")),
            "home_runs":   hr,
            "at_bats":     ab,
            "hits":        safe_int(stat.get("hits", 0)),
            "doubles":     dbl,
            "triples":     tri,
            "strike_outs": safe_int(stat.get("strikeOuts", 0)),
            "ingested_at": NOW.isoformat(),
        })
    return rows

# ── Step 5: Pitcher matchup ───────────────────────────────────────────────────
async def fetch_pitcher_matchup(session, pitcher_id, opp_team_id, game_pk, pitcher_name):
    data = await get(session, f"{BASE_URL}/mlb/pitcher-matchup", params={
        "pitcherId":      pitcher_id,
        "opposingTeamId": opp_team_id,
    })
    if not data or not isinstance(data, dict):
        return {"splits": [], "pitch_log": []}

    pitcher_hand = data.get("pitchingType", "")
    split_rows   = []

    for s in data.get("splits", []):
        split_rows.append({
            "run_date":      TODAY.isoformat(),
            "game_pk":       game_pk,
            "pitcher_id":    pitcher_id,
            "pitcher_name":  pitcher_name,
            "pitcher_hand":  pitcher_hand,
            "opp_team_id":   opp_team_id,
            "split":         s.get("split", ""),
            "ip":            safe_float(s.get("ip")),
            "home_runs":     safe_int(s.get("homeRuns")),
            "hr_per_9":      safe_float(s.get("homeRunsPer9Inn")),
            "barrel_pct":    safe_float(s.get("seasonBarrel")) or safe_float(s.get("seasonBarrelPct")),
            "hard_hit_pct":  safe_float(s.get("seasonHardHitPercentage")),
            "fb_pct":        safe_float(s.get("flyballPercentage")),
            "hr_fb_pct":     None,
            "whip":          safe_float(s.get("whip")),
            "woba":          safe_float(s.get("woba")),
            "ingested_at":   NOW.isoformat(),
        })

    pitch_log_rows = []
    for p in data.get("pitchLog", []):
        # Only store 2025 season pitch log
        if safe_int(p.get("season")) != 2025:
            continue
        pitch_log_rows.append({
            "run_date":    TODAY.isoformat(),
            "game_pk":     game_pk,
            "pitcher_id":  pitcher_id,
            "batter_hand": p.get("type", ""),
            "pitch_code":  p.get("pitchCode", ""),
            "pitch_name":  p.get("pitchName", ""),
            "season":      2025,
            "count":       safe_int(p.get("count")),
            "percentage":  safe_float(p.get("percentage")),
            "home_runs":   safe_int(p.get("homeRuns")),
            "woba":        safe_float(p.get("wOBA")),
            "slg":         safe_float(p.get("slg")),
            "iso":         safe_float(p.get("iso")),
            "whiff":       safe_float(p.get("whiff")),
            "k_percent":   safe_float(p.get("kPercent")),
            "ingested_at": NOW.isoformat(),
        })

    return {"splits": split_rows, "pitch_log": pitch_log_rows}

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    log.info(f"Starting PropFinder ingest for {TODAY}")

    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:

        # 1. Today's games
        games = await fetch_today_games(session)
        if not games:
            log.warning("No games found today — exiting")
            return

        # 2. Lineups for all games
        lineup_tasks = [fetch_lineup(session, g["game_pk"]) for g in games]
        lineup_results = await asyncio.gather(*lineup_tasks)
        lineups = {g["game_pk"]: r for g, r in zip(games, lineup_results)}

        # 3. Build batter list with team IDs and opposing pitcher
        # Key insight: home batters face away pitcher, away batters face home pitcher
        batter_jobs = []   # {batter_id, game_pk, batter_team_id, opp_pitcher_id}
        pitcher_jobs = []  # {pitcher_id, pitcher_name, opp_team_id, game_pk}

        for g in games:
            gp = g["game_pk"]
            lu = lineups.get(gp, {"home_players": [], "away_players": []})

            # Home batters (team=home_team_id) face away pitcher
            for bid in lu["home_players"]:
                batter_jobs.append({
                    "batter_id":      bid,
                    "game_pk":        gp,
                    "batter_team_id": g["home_team_id"],
                    "opp_pitcher_id": g["away_pitcher_id"],
                })

            # Away batters (team=away_team_id) face home pitcher
            for bid in lu["away_players"]:
                batter_jobs.append({
                    "batter_id":      bid,
                    "game_pk":        gp,
                    "batter_team_id": g["away_team_id"],
                    "opp_pitcher_id": g["home_pitcher_id"],
                })

            # Pitcher jobs
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

        # 4. Fetch batter hit-data + splits concurrently
        all_hit_rows   = []
        all_split_rows = []

        async def fetch_batter(job):
            hit_rows   = await fetch_hit_data(session, job["batter_id"], job["game_pk"], job["batter_team_id"])
            batter_name = hit_rows[0]["batter_name"] if hit_rows else str(job["batter_id"])
            split_rows = await fetch_splits(session, job["batter_id"], batter_name)
            return hit_rows, split_rows

        batter_results = await asyncio.gather(*[fetch_batter(j) for j in batter_jobs])
        for hit_rows, split_rows in batter_results:
            all_hit_rows.extend(hit_rows)
            all_split_rows.extend(split_rows)

        # 5. Fetch pitcher matchups concurrently
        all_pitcher_split_rows = []
        all_pitch_log_rows     = []

        pitcher_results = await asyncio.gather(*[
            fetch_pitcher_matchup(session, j["pitcher_id"], j["opp_team_id"], j["game_pk"], j["pitcher_name"])
            for j in pitcher_jobs
        ])
        for result in pitcher_results:
            all_pitcher_split_rows.extend(result["splits"])
            all_pitch_log_rows.extend(result["pitch_log"])

        # 6. Write to BigQuery
        bq_insert("raw_hit_data",        all_hit_rows)
        bq_insert("raw_splits",          all_split_rows)
        bq_insert("raw_pitcher_matchup", all_pitcher_split_rows)
        bq_insert("raw_pitch_log",       all_pitch_log_rows)

        log.info("Ingest complete.")

if __name__ == "__main__":
    asyncio.run(main())