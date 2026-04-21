"""
NBA PropFinder daily ingest — self-contained module for mobile_api.

Authenticates with PropFinder, fetches props + games + splits + game logs,
writes to BigQuery nba dataset.
"""

import datetime
import json
import logging
import os
import re
import time
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from google.cloud import bigquery

log = logging.getLogger("nba_propfinder_ingest")

# ── Config ────────────────────────────────────────────────────────────
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", os.getenv("GCP_PROJECT", "graphite-flare-477419-h7"))
DATASET = "nba"
PF_BASE = "https://api.propfinder.app"
PF_EMAIL = os.getenv("PROPFINDER_EMAIL", "Benvrana@bottleking.com")
PF_PASSWORD = os.getenv("PROPFINDER_PASSWORD", "Auzilla519!")

ET = ZoneInfo("America/New_York")

PROP_CATEGORIES = {
    "points", "rebounds", "assists", "threePointsMade",
    "pointsRebounds", "pointsAssists", "reboundAssists",
    "pointsReboundsAssists",
}


def _tbl(name):
    return f"`{PROJECT}.{DATASET}.{name}`"


def _api(path, token=None):
    url = f"{PF_BASE}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(url, headers=headers)
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def _api_post(path, body):
    url = f"{PF_BASE}{path}"
    data = json.dumps(body).encode()
    req = Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _safe_float(v, default=None):
    if v is None:
        return default
    try:
        return float(str(v))
    except (ValueError, TypeError):
        return default


def _safe_int(v, default=None):
    if v is None:
        return default
    try:
        return int(float(str(v)))
    except (ValueError, TypeError):
        return default


def _parse_dk_link(link):
    if not link:
        return None, None
    m = re.search(r"/event/(\d+)", link)
    event_id = m.group(1) if m else None
    m2 = re.search(r"outcomes=([^&]+)", link)
    outcome_code = m2.group(1) if m2 else None
    return event_id, outcome_code


def _parse_fd_link(link):
    if not link:
        return None, None
    m = re.search(r"marketId[^=]*=([^&]+)", link)
    market_id = m.group(1) if m else None
    m2 = re.search(r"selectionId[^=]*=([^&]+)", link)
    selection_id = m2.group(1) if m2 else None
    return market_id, selection_id


def _write_bq(bq, table_name, rows):
    if not rows:
        log.info(f"  {table_name}: 0 rows, skipping")
        return
    table_ref = f"{PROJECT}.{DATASET}.{table_name}"
    errors = bq.insert_rows_json(table_ref, rows)
    if errors:
        log.error(f"  BQ insert errors for {table_name}: {errors[:3]}")
    else:
        log.info(f"  {table_name}: {len(rows)} rows written")


def _clear_today(bq, table_name, today):
    sql = f"DELETE FROM {_tbl(table_name)} WHERE run_date = '{today}'"
    bq.query(sql).result()
    log.info(f"  Cleared {table_name} for {today}")


def authenticate():
    log.info("Authenticating with PropFinder...")
    resp = _api_post("/identity/authenticate", {
        "email": PF_EMAIL,
        "password": PF_PASSWORD,
    })
    token = resp.get("accessToken")
    if not token:
        raise RuntimeError("PropFinder auth failed — no access token")
    log.info("Authenticated successfully")
    return token


def ingest_props(bq, token, today, now):
    log.info("Fetching NBA props...")
    props = _api("/nba/props", token)
    log.info(f"  Received {len(props)} total props")

    rows = []
    for p in props:
        cat = p.get("category", "")
        if cat not in PROP_CATEGORIES:
            continue
        if p.get("isAlternate"):
            continue

        dk_price, dk_link, dk_event, dk_outcome = None, None, None, None
        fd_price, fd_link, fd_market, fd_selection = None, None, None, None
        best_book, best_price = None, None

        for mkt in p.get("markets", []):
            book = mkt.get("sportsbook", "")
            price = _safe_int(mkt.get("price"))
            if book == "DraftKings":
                dk_price = price
                dk_link = mkt.get("deepLinkDesktop", "")
                dk_event, dk_outcome = _parse_dk_link(dk_link)
            elif book == "FanDuel":
                fd_price = price
                fd_link = mkt.get("deepLinkDesktop", "")
                fd_market, fd_selection = _parse_fd_link(fd_link)

        if dk_price is not None and fd_price is not None:
            best_book, best_price = ("DraftKings", dk_price) if dk_price > fd_price else ("FanDuel", fd_price)
        elif dk_price is not None:
            best_book, best_price = "DraftKings", dk_price
        elif fd_price is not None:
            best_book, best_price = "FanDuel", fd_price

        if best_book is None:
            continue

        rows.append({
            "run_date": str(today), "prop_id": p.get("id"),
            "player_id": p.get("playerId"), "game_id": p.get("gameId"),
            "team_id": p.get("teamId"), "opp_team_id": p.get("opposingTeamId"),
            "player_name": p.get("name"), "team_code": p.get("teamCode"),
            "opp_team_code": p.get("opposingTeamCode"), "position": p.get("position"),
            "injury_status": p.get("injuryStatus"), "is_home": p.get("isHome"),
            "category": cat, "line": _safe_float(p.get("line")),
            "over_under": p.get("overUnder"), "is_alternate": False,
            "pf_rating": _safe_float(p.get("pfRating")),
            "matchup_rank": _safe_int(p.get("matchupRank")),
            "matchup_value": _safe_float(p.get("matchupValue")),
            "matchup_label": p.get("matchupLabel"),
            "hit_rate_l5": p.get("hitRateL5"), "hit_rate_l10": p.get("hitRateL10"),
            "hit_rate_l20": p.get("hitRateL20"), "hit_rate_season": p.get("hitRateSeason"),
            "hit_rate_last_season": p.get("hitRateLastSeason"),
            "hit_rate_vs_team": p.get("hitRateVsTeam"),
            "streak": p.get("streak"), "avg_l10": _safe_float(p.get("avgL10")),
            "avg_home_away": _safe_float(p.get("avgHomeAway")),
            "avg_vs_opponent": _safe_float(p.get("avgVsOpponent")),
            "best_book": best_book, "best_price": best_price,
            "best_line": _safe_float(p.get("line")),
            "dk_price": dk_price, "dk_deep_link": dk_link,
            "dk_outcome_code": dk_outcome, "dk_event_id": dk_event,
            "fd_price": fd_price, "fd_deep_link": fd_link,
            "fd_market_id": fd_market, "fd_selection_id": fd_selection,
            "ingested_at": now.isoformat(),
        })

    _clear_today(bq, "raw_nba_props", today)
    _write_bq(bq, "raw_nba_props", rows)
    return rows


def ingest_games(bq, token, today, now):
    log.info("Fetching NBA upcoming games...")
    games = _api("/nba/upcoming-games", token)
    log.info(f"  Received {len(games)} games")

    rows = []
    for g in games:
        home = g.get("homeTeam", {})
        away = g.get("visitorTeam", {})
        rows.append({
            "run_date": str(today), "game_id": g.get("id"),
            "game_date": g.get("gameDate"),
            "home_team_id": home.get("id"), "home_team_code": home.get("code"),
            "home_team_name": home.get("fullName"),
            "away_team_id": away.get("id"), "away_team_code": away.get("code"),
            "away_team_name": away.get("fullName"),
            "home_ml": g.get("homeTeamOdds"), "away_ml": g.get("visitorTeamOdds"),
            "home_spread_line": g.get("homeTeamSpreadLine"),
            "home_spread_odds": g.get("homeTeamSpreadOdds"),
            "away_spread_line": g.get("visitorTeamSpreadLine"),
            "away_spread_odds": g.get("visitorTeamSpreadOdds"),
            "total_line": _safe_float(g.get("gameRunLine")),
            "ingested_at": now.isoformat(),
        })

    _clear_today(bq, "raw_nba_games", today)
    _write_bq(bq, "raw_nba_games", rows)
    return rows


def ingest_splits(bq, token, player_ids, today, now):
    log.info(f"Fetching splits for {len(player_ids)} players...")
    rows = []
    done = 0
    for pid in player_ids:
        try:
            data = _api(f"/nba/splits?playerId={pid}", token)
            if not data or not isinstance(data, dict):
                continue
            rows.append({
                "run_date": str(today), "player_id": pid,
                "player_name": data.get("name"), "position": data.get("position"),
                "season_year": _safe_int(data.get("seasonYear")),
                "season_type": data.get("seasonType"),
                "games_played": _safe_int(data.get("gamesPlayed")),
                "games_started": _safe_int(data.get("gamesStarted")),
                "minutes": _safe_float(data.get("minutes")),
                "points": _safe_int(data.get("points")),
                "rebounds": _safe_int(data.get("rebounds")),
                "offensive_rebounds": _safe_int(data.get("offensiveRebounds")),
                "defensive_rebounds": _safe_int(data.get("defensiveRebounds")),
                "assists": _safe_int(data.get("assists")),
                "steals": _safe_int(data.get("steals")),
                "blocks": _safe_int(data.get("blocks")),
                "turnovers": _safe_int(data.get("turnovers")),
                "field_goals_made": _safe_int(data.get("fieldGoalsMade")),
                "field_goals_att": _safe_int(data.get("fieldGoalsAtt")),
                "field_goals_pct": _safe_float(data.get("fieldGoalsPct")),
                "three_points_made": _safe_int(data.get("threePointsMade")),
                "three_points_att": _safe_int(data.get("threePointsAtt")),
                "three_points_pct": _safe_float(data.get("threePointsPct")),
                "free_throws_made": _safe_int(data.get("freeThrowsMade")),
                "free_throws_att": _safe_int(data.get("freeThrowsAtt")),
                "free_throws_pct": _safe_float(data.get("freeThrowsPct")),
                "two_points_made": _safe_int(data.get("twoPointsMade")),
                "two_points_att": _safe_int(data.get("twoPointsAtt")),
                "two_points_pct": _safe_float(data.get("twoPointsPct")),
                "usage_pct": _safe_float(data.get("usagePct")),
                "true_shooting_pct": _safe_float(data.get("trueShootingPct")),
                "effective_fg_pct": _safe_float(data.get("effectiveFgPct")),
                "assists_turnover_ratio": _safe_float(data.get("assistsTurnoverRatio")),
                "efficiency": _safe_int(data.get("efficiency")),
                "points_in_paint": _safe_int(data.get("pointsInPaint")),
                "fast_break_pts": _safe_int(data.get("fastBreakPts")),
                "second_chance_pts": _safe_int(data.get("secondChancePts")),
                "plus": _safe_int(data.get("plus")),
                "minus": _safe_int(data.get("minus")),
                "double_doubles": _safe_int(data.get("doubleDoubles")),
                "fouls_drawn": _safe_int(data.get("foulsDrawn")),
                "fg_at_rim_made": _safe_int(data.get("fieldGoalsAtRimMade")),
                "fg_at_rim_att": _safe_int(data.get("fieldGoalsAtRimAtt")),
                "fg_at_rim_pct": _safe_float(data.get("fieldGoalsAtRimPct")),
                "fg_midrange_made": _safe_int(data.get("fieldGoalsAtMidrangeMade")),
                "fg_midrange_att": _safe_int(data.get("fieldGoalsAtMidrangeAtt")),
                "fg_midrange_pct": _safe_float(data.get("fieldGoalsAtMidrangePct")),
                "ingested_at": now.isoformat(),
            })
        except Exception as e:
            log.warning(f"  Splits failed for {pid}: {e}")
        done += 1
        if done % 20 == 0:
            log.info(f"  Splits progress: {done}/{len(player_ids)}")
            time.sleep(0.3)

    _clear_today(bq, "raw_nba_splits", today)
    _write_bq(bq, "raw_nba_splits", rows)
    return rows


def ingest_game_logs(bq, token, team_ids, today, now):
    log.info(f"Fetching game logs for {len(team_ids)} teams...")
    rows = []
    seen = set()

    for tid in team_ids:
        try:
            players = _api(f"/nba/players?teamIds={tid}", token)
            if not isinstance(players, list):
                continue
            for player in players:
                pid = player.get("id")
                for stat in player.get("stats", []):
                    game_id = stat.get("gameId")
                    key = (pid, game_id)
                    if key in seen:
                        continue
                    seen.add(key)

                    pts = _safe_int(stat.get("points"), 0)
                    reb = _safe_int(stat.get("rebounds"), 0)
                    ast = _safe_int(stat.get("assists"), 0)
                    tpm = _safe_int(stat.get("threePointsMade"), 0)
                    stl = _safe_int(stat.get("steals"), 0)
                    blk = _safe_int(stat.get("blocks"), 0)

                    rows.append({
                        "run_date": str(today), "player_id": pid,
                        "player_name": stat.get("fullName") or stat.get("playerName") or player.get("name"),
                        "position": stat.get("primaryPosition") or stat.get("playerPosition") or player.get("position"),
                        "team_id": stat.get("teamId") or tid,
                        "team_code": stat.get("teamCode"),
                        "opp_team_id": stat.get("opposingTeamId"),
                        "opp_team_code": stat.get("opposingTeamCode"),
                        "game_id": game_id,
                        "game_date": (stat.get("gameDate") or "")[:10] or None,
                        "season": _safe_int(stat.get("season")),
                        "season_type": stat.get("seasonType"),
                        "is_home": stat.get("isHomeGame"),
                        "is_win": stat.get("isWin"),
                        "point_differential": _safe_int(stat.get("pointDifferential")),
                        "minutes": _safe_float(stat.get("minutes")),
                        "points": pts, "rebounds": reb, "assists": ast,
                        "offensive_rebounds": _safe_int(stat.get("offensiveRebounds")),
                        "defensive_rebounds": _safe_int(stat.get("defensiveRebounds")),
                        "steals": stl, "blocks": blk,
                        "turnovers": _safe_int(stat.get("turnovers")),
                        "field_goals_made": _safe_int(stat.get("fieldGoalsMade")),
                        "field_goals_att": _safe_int(stat.get("fieldGoalsAtt")),
                        "field_goals_pct": _safe_float(stat.get("fieldGoalsPct")),
                        "three_points_made": tpm,
                        "three_points_att": _safe_int(stat.get("threePointsAtt")),
                        "two_points_made": _safe_int(stat.get("twoPointsMade")),
                        "two_points_att": _safe_int(stat.get("twoPointsAtt")),
                        "free_throws_made": _safe_int(stat.get("freeThrowsMade")),
                        "free_throws_att": _safe_int(stat.get("freeThrowsAtt")),
                        "steals_blocks": _safe_int(stat.get("stealsBlocks"), stl + blk),
                        "points_rebounds_assists": _safe_int(stat.get("pointsReboundsAssists"), pts + reb + ast),
                        "points_assists": _safe_int(stat.get("pointsAssists"), pts + ast),
                        "rebound_assists": _safe_int(stat.get("reboundAssists"), reb + ast),
                        "points_rebounds": _safe_int(stat.get("pointsRebounds"), pts + reb),
                        "usage_pct": _safe_float(stat.get("usagePct")),
                        "efficiency": _safe_int(stat.get("efficiency")),
                        "contested_rebounds": _safe_int(stat.get("contestedRebounds")),
                        "potential_assists": _safe_int(stat.get("potentialAssists")),
                        "effective_fg_pct": _safe_float(stat.get("effectiveFgPct")),
                        "ingested_at": now.isoformat(),
                    })
        except Exception as e:
            log.warning(f"  Game logs failed for team {tid}: {e}")
        time.sleep(0.5)

    _clear_today(bq, "raw_nba_game_logs", today)
    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        _write_bq(bq, "raw_nba_game_logs", rows[i:i + batch_size])
    log.info(f"  Total game log rows: {len(rows)}")
    return rows


def run_daily_ingest():
    """Run the full NBA PropFinder ingest (props, games, splits, game logs)."""
    today = datetime.datetime.now(ET).date()
    now = datetime.datetime.now(datetime.timezone.utc)

    log.info(f"=== NBA PropFinder Ingest {today} ===")

    bq = bigquery.Client(project=PROJECT)
    token = authenticate()

    # 1) Props
    props = ingest_props(bq, token, today, now)

    # 2) Games
    games = ingest_games(bq, token, today, now)

    # Collect unique player/team IDs
    player_ids = list({p["player_id"] for p in props if p.get("player_id")})
    team_ids = set()
    for g in games:
        if g.get("home_team_id"):
            team_ids.add(g["home_team_id"])
        if g.get("away_team_id"):
            team_ids.add(g["away_team_id"])

    # 3) Splits
    ingest_splits(bq, token, player_ids, today, now)

    # 4) Game logs
    ingest_game_logs(bq, token, list(team_ids), today, now)

    log.info("=== NBA PropFinder Ingest complete ===")
    return {"status": "ok", "props": len(props), "games": len(games)}
