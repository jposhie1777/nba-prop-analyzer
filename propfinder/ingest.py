"""
ingest.py — PropFinder data ingestion pipeline
Fetches today's MLB games, lineups, batter hit-data, splits, and pitcher matchup data.
Writes raw data to BigQuery propfinder dataset.
"""

import asyncio
import datetime
import json
import logging
import os
from typing import Optional

import aiohttp
from google.cloud import bigquery

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT   = "graphite-flare-477419-h7"
DATASET   = "propfinder"
BASE_URL  = "https://api.propfinder.app"
MLB_API   = "https://statsapi.mlb.com/api/v1"
TODAY     = datetime.date.today()
NOW       = datetime.datetime.utcnow()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bq = bigquery.Client(project=PROJECT)

# ── Helpers ───────────────────────────────────────────────────────────────────
def table(name: str):
    return f"{PROJECT}.{DATASET}.{name}"

async def get(session: aiohttp.ClientSession, url: str, params: dict = None) -> Optional[dict | list]:
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            log.warning(f"HTTP {r.status} for {url} params={params}")
            return None
    except Exception as e:
        log.error(f"Request failed {url}: {e}")
        return None

def safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None

def safe_int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None

def iso_date(s: str) -> Optional[datetime.date]:
    try:
        return datetime.date.fromisoformat(s[:10])
    except Exception:
        return None

def bq_insert(table_name: str, rows: list[dict]):
    if not rows:
        return
    errors = bq.insert_rows_json(table(table_name), rows)
    if errors:
        log.error(f"BQ insert errors for {table_name}: {errors[:3]}")
    else:
        log.info(f"Inserted {len(rows)} rows into {table_name}")

# ── Step 1: Today's games ─────────────────────────────────────────────────────
async def fetch_today_games(session: aiohttp.ClientSession) -> list[dict]:
    """Returns list of {game_pk, home_team_id, away_team_id, home_team_name,
    away_team_name, home_pitcher_id, away_pitcher_id, home_pitcher_name, away_pitcher_name}"""
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
                "game_pk":            g["gamePk"],
                "home_team_id":       home["team"]["id"],
                "away_team_id":       away["team"]["id"],
                "home_team_name":     home["team"]["name"],
                "away_team_name":     away["team"]["name"],
                "home_pitcher_id":    home.get("probablePitcher", {}).get("id"),
                "home_pitcher_name":  home.get("probablePitcher", {}).get("fullName", "TBD"),
                "away_pitcher_id":    away.get("probablePitcher", {}).get("id"),
                "away_pitcher_name":  away.get("probablePitcher", {}).get("fullName", "TBD"),
            })
    log.info(f"Found {len(games)} games for {TODAY}")
    return games

# ── Step 2: Lineups ───────────────────────────────────────────────────────────
async def fetch_lineup(session: aiohttp.ClientSession, game_pk: int) -> dict:
    """Returns {home_players: [id,...], away_players: [id,...]}"""
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

async def fetch_all_lineups(session: aiohttp.ClientSession, games: list[dict]) -> dict[int, dict]:
    """Returns {game_pk: {home_players, away_players}}"""
    tasks = [fetch_lineup(session, g["game_pk"]) for g in games]
    results = await asyncio.gather(*tasks)
    return {g["game_pk"]: r for g, r in zip(games, results)}

# ── Step 3: Batter hit-data ───────────────────────────────────────────────────
async def fetch_hit_data(session: aiohttp.ClientSession, batter_id: int, game_pk: int) -> list[dict]:
    data = await get(session, f"{BASE_URL}/mlb/hit-data",
                     params={"playerId": batter_id, "group": "hitting"})
    if not data or not isinstance(data, list):
        return []

    rows = []
    for ev in data:
        launch_speed = safe_float(ev.get("launchSpeed"))
        if launch_speed is None:
            continue  # skip events with no contact data
        rows.append({
            "run_date":       TODAY.isoformat(),
            "game_pk":        game_pk,
            "batter_id":      batter_id,
            "batter_name":    ev.get("batterName", ""),
            "bat_side":       ev.get("batSide", ""),
            "pitcher_id":     safe_int(ev.get("pitcherId")),
            "pitcher_name":   ev.get("pitcherName", ""),
            "pitch_hand":     ev.get("pitchHand", ""),
            "pitch_type":     ev.get("pitchType", ""),
            "result":         ev.get("result", ""),
            "launch_speed":   launch_speed,
            "launch_angle":   safe_float(ev.get("launchAngle")),
            "total_distance": safe_float(ev.get("totalDistance")),
            "trajectory":     ev.get("trajectory", ""),
            "is_barrel":      bool(ev.get("isBarrel")),
            "hr_in_n_parks":  safe_int(ev.get("hrInNParks", 0)),
            "event_date":     ev.get("date", "")[:10],
            "season":         safe_int(ev.get("season")),
            "ingested_at":    NOW.isoformat(),
        })
    return rows

# ── Step 4: Batter splits ─────────────────────────────────────────────────────
SPLITS_WANTED = {"vl", "vr", "h", "a", "r", "g", "t", "preas", "posas"}

async def fetch_splits(session: aiohttp.ClientSession, batter_id: int, batter_name: str) -> list[dict]:
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
        hits = safe_int(stat.get("hits", 0)) or 0
        hr   = safe_int(stat.get("homeRuns", 0)) or 0
        doubles = safe_int(stat.get("doubles", 0)) or 0
        triples = safe_int(stat.get("triples", 0)) or 0
        # ISO = (2B + 2*3B + 3*HR) / AB
        iso = round((doubles + 2 * triples + 3 * hr) / ab, 3) if ab > 0 else 0.0
        rows.append({
            "run_date":    TODAY.isoformat(),
            "batter_id":   batter_id,
            "batter_name": batter_name,
            "split_code":  code,
            "split_name":  s.get("splitName", ""),
            "season":      str(s.get("season", "")),
            "avg":         safe_float(str(stat.get("avg", "0")).replace(".", "0.", 1) if str(stat.get("avg","")).startswith(".") else stat.get("avg", 0)),
            "obp":         safe_float(str(stat.get("obp", "0")).replace(".", "0.", 1) if str(stat.get("obp","")).startswith(".") else stat.get("obp", 0)),
            "slg":         safe_float(str(stat.get("slg", "0")).replace(".", "0.", 1) if str(stat.get("slg","")).startswith(".") else stat.get("slg", 0)),
            "ops":         safe_float(str(stat.get("ops", "0")).replace(".", "0.", 1) if str(stat.get("ops","")).startswith(".") else stat.get("ops", 0)),
            "home_runs":   hr,
            "at_bats":     ab,
            "hits":        hits,
            "doubles":     doubles,
            "triples":     triples,
            "strike_outs": safe_int(stat.get("strikeOuts", 0)),
            "ingested_at": NOW.isoformat(),
        })
    return rows

# ── Step 5: Pitcher matchup ───────────────────────────────────────────────────
async def fetch_pitcher_matchup(
    session: aiohttp.ClientSession,
    pitcher_id: int,
    opp_team_id: int,
    game_pk: int,
    pitcher_name: str,
) -> dict:
    """Returns {splits: [...], pitch_log: [...]}"""
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
            "hr_fb_pct":     None,  # computed in model
            "whip":          safe_float(s.get("whip")),
            "woba":          safe_float(s.get("woba")),
            "ingested_at":   NOW.isoformat(),
        })

    pitch_log_rows = []
    for p in data.get("pitchLog", []):
        pitch_log_rows.append({
            "run_date":    TODAY.isoformat(),
            "game_pk":     game_pk,
            "pitcher_id":  pitcher_id,
            "batter_hand": p.get("type", ""),
            "pitch_code":  p.get("pitchCode", ""),
            "pitch_name":  p.get("pitchName", ""),
            "season":      safe_int(p.get("season")),
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

        # 2. Lineups
        lineups = await fetch_all_lineups(session, games)

        # Build batter list: {batter_id: {game_pk, team_side, opp_pitcher_id, opp_pitcher_name}}
        batter_map: dict[int, dict] = {}
        pitcher_jobs: list[dict] = []

        for g in games:
            gp = g["game_pk"]
            lu = lineups.get(gp, {"home_players": [], "away_players": []})

            # Home batters face away pitcher
            for bid in lu["home_players"]:
                batter_map[bid] = {
                    "game_pk":        gp,
                    "opp_pitcher_id": g["away_pitcher_id"],
                    "home_team":      g["home_team_name"],
                    "away_team":      g["away_team_name"],
                }
            # Away batters face home pitcher
            for bid in lu["away_players"]:
                batter_map[bid] = {
                    "game_pk":        gp,
                    "opp_pitcher_id": g["home_pitcher_id"],
                    "home_team":      g["home_team_name"],
                    "away_team":      g["away_team_name"],
                }

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

        # If lineups not yet posted, fall back to roster-based batter list
        # via the MLB statsapi probable pitchers (batters TBD until lineup posted)
        log.info(f"Batter IDs from lineups: {len(batter_map)}")
        log.info(f"Pitcher jobs: {len(pitcher_jobs)}")

        # 3. Fetch batter hit-data + splits concurrently
        all_hit_rows:   list[dict] = []
        all_split_rows: list[dict] = []

        async def fetch_batter(bid: int, info: dict):
            gp = info["game_pk"]
            hit_rows   = await fetch_hit_data(session, bid, gp)
            split_rows = await fetch_splits(session, bid, hit_rows[0]["batter_name"] if hit_rows else str(bid))
            return hit_rows, split_rows

        batter_tasks = [fetch_batter(bid, info) for bid, info in batter_map.items()]
        batter_results = await asyncio.gather(*batter_tasks)
        for hit_rows, split_rows in batter_results:
            all_hit_rows.extend(hit_rows)
            all_split_rows.extend(split_rows)

        # 4. Fetch pitcher matchups concurrently
        all_pitcher_split_rows:    list[dict] = []
        all_pitch_log_rows:        list[dict] = []

        async def fetch_pitcher(job: dict):
            result = await fetch_pitcher_matchup(
                session,
                job["pitcher_id"],
                job["opp_team_id"],
                job["game_pk"],
                job["pitcher_name"],
            )
            return result

        pitcher_results = await asyncio.gather(*[fetch_pitcher(j) for j in pitcher_jobs])
        for result in pitcher_results:
            all_pitcher_split_rows.extend(result["splits"])
            all_pitch_log_rows.extend(result["pitch_log"])

        # 5. Write to BigQuery
        bq_insert("raw_hit_data",         all_hit_rows)
        bq_insert("raw_splits",           all_split_rows)
        bq_insert("raw_pitcher_matchup",  all_pitcher_split_rows)
        bq_insert("raw_pitch_log",        all_pitch_log_rows)

        log.info("Ingest complete.")

if __name__ == "__main__":
    asyncio.run(main())