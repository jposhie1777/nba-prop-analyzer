"""
ingest.py - PropFinder data ingestion pipeline
Fetches today's MLB games, lineups, batter hit-data, splits, and pitcher matchup data.
Writes raw data to BigQuery propfinder dataset.

v3 fixes:
- pitcher splits: 2025 only
- barrel_pct, hard_hit_pct, fb_pct pulled from metricAverages
- HR/FB% computed from homeRuns / airOuts
- batter_team_id stored so model can correctly match batters to pitchers
"""

import asyncio
import datetime
import logging

import aiohttp
from google.cloud import bigquery

PROJECT = "graphite-flare-477419-h7"
DATASET = "propfinder"
BASE_URL = "https://api.propfinder.app"
MLB_API = "https://statsapi.mlb.com/api/v1"
TODAY = datetime.date.today()
NOW = datetime.datetime.now(datetime.timezone.utc)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
bq = bigquery.Client(project=PROJECT)


def table(name):
    return f"{PROJECT}.{DATASET}.{name}"


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


def bq_insert(table_name, rows):
    if not rows:
        log.info("No rows for %s", table_name)
        return
    errors = bq.insert_rows_json(table(table_name), rows)
    if errors:
        log.error("BQ insert errors %s: %s", table_name, errors[:3])
    else:
        log.info("Inserted %s rows -> %s", len(rows), table_name)


async def fetch_today_games(session):
    data = await get(
        session,
        f"{MLB_API}/schedule",
        params={
            "sportId": 1,
            "date": TODAY.isoformat(),
            "hydrate": "probablePitcher,team",
        },
    )
    if not data:
        return []

    games = []
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            home = game["teams"]["home"]
            away = game["teams"]["away"]
            games.append(
                {
                    "game_pk": game["gamePk"],
                    "home_team_id": home["team"]["id"],
                    "away_team_id": away["team"]["id"],
                    "home_team_name": home["team"]["name"],
                    "away_team_name": away["team"]["name"],
                    "home_pitcher_id": home.get("probablePitcher", {}).get("id"),
                    "home_pitcher_name": home.get("probablePitcher", {}).get(
                        "fullName", "TBD"
                    ),
                    "away_pitcher_id": away.get("probablePitcher", {}).get("id"),
                    "away_pitcher_name": away.get("probablePitcher", {}).get(
                        "fullName", "TBD"
                    ),
                }
            )

    log.info("Found %s games for %s", len(games), TODAY)
    return games


async def fetch_lineup(session, game_pk):
    data = await get(session, f"{BASE_URL}/mlb/lineups", params={"gameIds": game_pk})
    if not data or not isinstance(data, list):
        return {"home_players": [], "away_players": []}

    for item in data:
        if si(item.get("gamePk")) == si(game_pk):
            lineups = item.get("lineups", {})
            return {
                "home_players": unique_ints(
                    extract_player_id(player)
                    for player in lineups.get("homePlayers", [])
                ),
                "away_players": unique_ints(
                    extract_player_id(player)
                    for player in lineups.get("awayPlayers", [])
                ),
            }
    return {"home_players": [], "away_players": []}


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

        rows.append(
            {
                "run_date": TODAY.isoformat(),
                "game_pk": game_pk,
                "batter_id": batter_id,
                "batter_team_id": batter_team_id,
                "batter_name": event.get("batterName", ""),
                "bat_side": event.get("batSide", ""),
                "pitcher_id": si(event.get("pitcherId")),
                "pitcher_name": event.get("pitcherName", ""),
                "pitch_hand": event.get("pitchHand", ""),
                "pitch_type": event.get("pitchType", ""),
                "result": event.get("result", ""),
                "launch_speed": launch_speed,
                "launch_angle": sf(event.get("launchAngle")),
                "total_distance": sf(event.get("totalDistance")),
                "trajectory": event.get("trajectory", ""),
                "is_barrel": bool(event.get("isBarrel")),
                "hr_in_n_parks": si(event.get("hrInNParks", 0)),
                "event_date": event.get("date", "")[:10],
                "season": si(event.get("season")),
                "ingested_at": NOW.isoformat(),
            }
        )
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

        stat = split_row.get("stat", {})
        at_bats = si(stat.get("atBats", 0)) or 0
        home_runs = si(stat.get("homeRuns", 0)) or 0
        doubles = si(stat.get("doubles", 0)) or 0
        triples = si(stat.get("triples", 0)) or 0

        rows.append(
            {
                "run_date": TODAY.isoformat(),
                "batter_id": batter_id,
                "batter_name": batter_name,
                "split_code": code,
                "split_name": split_row.get("splitName", ""),
                "season": str(split_row.get("season", "")),
                "avg": parse_split_float(stat.get("avg")),
                "obp": parse_split_float(stat.get("obp")),
                "slg": parse_split_float(stat.get("slg")),
                "ops": parse_split_float(stat.get("ops")),
                "home_runs": home_runs,
                "at_bats": at_bats,
                "hits": si(stat.get("hits", 0)),
                "doubles": doubles,
                "triples": triples,
                "strike_outs": si(stat.get("strikeOuts", 0)),
                "ingested_at": NOW.isoformat(),
            }
        )
    return rows


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
        if si(split_row.get("season")) != 2025:
            continue

        split_key = split_row.get("split", "Season")
        metrics = metric_map.get(split_key, metric_map.get("Season", {}))

        barrel_pct = pct_from_metric(metrics.get("seasonBarrel"))
        hard_hit_pct = pct_from_metric(metrics.get("seasonHardHitPercentage"))
        fb_pct = pct_from_metric(metrics.get("flyballPercentage"))

        hr_n = si(split_row.get("homeRuns")) or 0
        air_outs = si(split_row.get("airOuts")) or 0
        hr_fb_pct = round((hr_n / air_outs) * 100, 2) if air_outs > 0 else 0.0

        split_rows.append(
            {
                "run_date": TODAY.isoformat(),
                "game_pk": game_pk,
                "pitcher_id": pitcher_id,
                "pitcher_name": pitcher_name,
                "pitcher_hand": pitcher_hand,
                "opp_team_id": opp_team_id,
                "split": split_key,
                "ip": sf(split_row.get("ip")),
                "home_runs": hr_n,
                "hr_per_9": sf(split_row.get("homeRunsPer9Inn")),
                "barrel_pct": round(barrel_pct, 4),
                "hard_hit_pct": round(hard_hit_pct, 4),
                "fb_pct": round(fb_pct, 4),
                "hr_fb_pct": hr_fb_pct,
                "whip": sf(split_row.get("whip")),
                "woba": sf(split_row.get("woba")),
                "ingested_at": NOW.isoformat(),
            }
        )

    pitch_log_rows = []
    for pitch in data.get("pitchLog", []):
        if si(pitch.get("season")) != 2025:
            continue

        pitch_log_rows.append(
            {
                "run_date": TODAY.isoformat(),
                "game_pk": game_pk,
                "pitcher_id": pitcher_id,
                "batter_hand": pitch.get("type", ""),
                "pitch_code": pitch.get("pitchCode", ""),
                "pitch_name": pitch.get("pitchName", ""),
                "season": 2025,
                "count": si(pitch.get("count")),
                "percentage": sf(pitch.get("percentage")),
                "home_runs": si(pitch.get("homeRuns")),
                "woba": sf(pitch.get("wOBA")),
                "slg": sf(pitch.get("slg")),
                "iso": sf(pitch.get("iso")),
                "whiff": sf(pitch.get("whiff")),
                "k_percent": sf(pitch.get("kPercent")),
                "ingested_at": NOW.isoformat(),
            }
        )

    return {"splits": split_rows, "pitch_log": pitch_log_rows}


def load_recent_team_batters(team_ids, lookback_days=365, per_team=9):
    team_ids = unique_ints(team_ids)
    if not team_ids:
        return {}

    try:
        sql = f"""
        WITH recent AS (
          SELECT
            batter_team_id,
            batter_id,
            COUNT(*) AS events,
            MAX(event_date) AS last_event_date
          FROM `{PROJECT}.{DATASET}.raw_hit_data`
          WHERE batter_team_id IN UNNEST(@team_ids)
            AND run_date >= DATE_SUB(@run_date, INTERVAL @lookback_days DAY)
          GROUP BY batter_team_id, batter_id
        ),
        ranked AS (
          SELECT
            batter_team_id,
            batter_id,
            ROW_NUMBER() OVER (
              PARTITION BY batter_team_id
              ORDER BY events DESC, last_event_date DESC, batter_id DESC
            ) AS rn
          FROM recent
        )
        SELECT batter_team_id, batter_id
        FROM ranked
        WHERE rn <= @per_team
        ORDER BY batter_team_id, rn
        """
        params = [
            bigquery.ArrayQueryParameter("team_ids", "INT64", team_ids),
            bigquery.ScalarQueryParameter("run_date", "DATE", TODAY.isoformat()),
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
            bigquery.ScalarQueryParameter("per_team", "INT64", per_team),
        ]
        rows = bq.query(
            sql, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result()
    except Exception as exc:
        log.warning("Fallback recent-team batter lookup failed: %s", exc)
        return {}

    by_team = {}
    for row in rows:
        team_id = si(row["batter_team_id"])
        batter_id = si(row["batter_id"])
        if team_id is None or batter_id is None:
            continue
        by_team.setdefault(team_id, []).append(batter_id)

    total = sum(len(v) for v in by_team.values())
    log.info(
        "Loaded %s fallback batters across %s teams from recent raw_hit_data",
        total,
        len(by_team),
    )
    return by_team


async def main():
    log.info("Starting PropFinder ingest for %s", TODAY)

    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        games = await fetch_today_games(session)
        if not games:
            log.warning("No games found - exiting")
            return

        lineup_results = await asyncio.gather(
            *[fetch_lineup(session, game["game_pk"]) for game in games]
        )
        lineups = {game["game_pk"]: result for game, result in zip(games, lineup_results)}
        fallback_batters = load_recent_team_batters(
            [game["home_team_id"] for game in games]
            + [game["away_team_id"] for game in games]
        )

        batter_jobs = []
        batter_seen = set()
        pitcher_jobs = []

        for game in games:
            game_pk = game["game_pk"]
            lineup = lineups.get(game_pk, {"home_players": [], "away_players": []})
            home_players = lineup.get("home_players", [])
            away_players = lineup.get("away_players", [])
            home_source = "lineup"
            away_source = "lineup"

            if not home_players:
                home_players = fallback_batters.get(game["home_team_id"], [])
                if home_players:
                    home_source = "recent-history"
            if not away_players:
                away_players = fallback_batters.get(game["away_team_id"], [])
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
                batter_jobs.append(
                    {"batter_id": batter_id_int, "game_pk": game_pk, "batter_team_id": game["home_team_id"]}
                )
            for batter_id in away_players:
                batter_id_int = si(batter_id)
                if batter_id_int is None:
                    continue
                key = (batter_id_int, game_pk, game["away_team_id"])
                if key in batter_seen:
                    continue
                batter_seen.add(key)
                batter_jobs.append(
                    {"batter_id": batter_id_int, "game_pk": game_pk, "batter_team_id": game["away_team_id"]}
                )

            if game["home_pitcher_id"]:
                pitcher_jobs.append(
                    {
                        "pitcher_id": game["home_pitcher_id"],
                        "pitcher_name": game["home_pitcher_name"],
                        "opp_team_id": game["away_team_id"],
                        "game_pk": game_pk,
                    }
                )
            if game["away_pitcher_id"]:
                pitcher_jobs.append(
                    {
                        "pitcher_id": game["away_pitcher_id"],
                        "pitcher_name": game["away_pitcher_name"],
                        "opp_team_id": game["home_team_id"],
                        "game_pk": game_pk,
                    }
                )

        log.info("Batter jobs: %s | Pitcher jobs: %s", len(batter_jobs), len(pitcher_jobs))

        all_hit_rows = []
        all_split_rows = []

        async def fetch_batter(job):
            hit_rows = await fetch_hit_data(
                session, job["batter_id"], job["game_pk"], job["batter_team_id"]
            )
            batter_name = hit_rows[0]["batter_name"] if hit_rows else str(job["batter_id"])
            split_rows = await fetch_splits(session, job["batter_id"], batter_name)
            return hit_rows, split_rows

        batter_results = await asyncio.gather(*[fetch_batter(job) for job in batter_jobs])
        for hit_rows, split_rows in batter_results:
            all_hit_rows.extend(hit_rows)
            all_split_rows.extend(split_rows)

        all_pitcher_rows = []
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

        bq_insert("raw_hit_data", all_hit_rows)
        bq_insert("raw_splits", all_split_rows)
        bq_insert("raw_pitcher_matchup", all_pitcher_rows)
        bq_insert("raw_pitch_log", all_pitch_log_rows)

        log.info("Ingest complete.")


if __name__ == "__main__":
    asyncio.run(main())
