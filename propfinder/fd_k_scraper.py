"""
fd_k_scraper.py — FanDuel pitcher strikeout under/over scraper.

Hits FanDuel's public sbapi to fetch pitcher K prop markets (over AND under)
for today's MLB games. Merges under lines into raw_k_props so the K model
can score both sides.

The PropFinder API only returns overs; FanDuel gives us both sides with
market IDs and selection IDs for deep-link building.

Usage:
  python propfinder/fd_k_scraper.py              # scrape + write to BQ
  python propfinder/fd_k_scraper.py --dry-run    # print results, no BQ write
"""

import argparse
import datetime
import json
import logging
import os
import time
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "graphite-flare-477419-h7")
DATASET = "propfinder"
SLATE_TZ = ZoneInfo("America/New_York")
TODAY = datetime.datetime.now(SLATE_TZ).date()
NOW = datetime.datetime.now(datetime.timezone.utc)

FD_API_KEY = os.getenv("FD_API_KEY", "FhMFpcPWXMeyZxOx")
FD_STATE = os.getenv("FD_STATE", "nj")
FD_MLB_EVENT_TYPE_ID = "7511"

FD_BASE_URL = "https://sportsbook.fanduel.com"

# FanDuel valid states
FD_VALID_STATES = {
    "nj", "pa", "il", "az", "co", "mi", "va", "in", "tn", "ny",
    "dc", "wv", "ky", "ia", "ct", "la", "md", "ma", "oh", "nc", "vt",
}

# Market type patterns for pitcher K props (standard O/U lines)
K_STANDARD_MARKET_TYPES = {"TOTAL_STRIKEOUTS"}  # matches PITCHER_C_TOTAL_STRIKEOUTS etc.
# Alt K markets (X+ Strikeouts)
K_ALT_MARKET_TYPES = {"STRIKEOUTS"}  # matches PITCHER_C_STRIKEOUTS etc. but NOT TOTAL_STRIKEOUTS

# MLB competition ID on FanDuel
FD_MLB_COMP_ID = "11196870"

bq_client = bigquery.Client(project=PROJECT)


def _fd_api_base(state: str = "nj") -> str:
    s = (state or "nj").lower().strip()
    if s not in FD_VALID_STATES:
        s = "nj"
    return f"https://sbapi.{s}.sportsbook.fanduel.com/api"


def _api_get(url: str) -> dict:
    req = Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    })
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def sf(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def si(val, default=0):
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


# ── Step 1: Get today's MLB events from FanDuel ─────────────────────────

def fetch_fd_mlb_events(state: str = "nj"):
    """Fetch all MLB events for today from FanDuel."""
    base = _fd_api_base(state)
    url = (
        f"{base}/content-managed-page"
        f"?page=SPORT&eventTypeId={FD_MLB_EVENT_TYPE_ID}&_ak={FD_API_KEY}"
    )
    try:
        data = _api_get(url)
        events = data.get("attachments", {}).get("events", {})
        # Filter to real MLB games only (comp ID 11196870), skip NCAA/futures/NPB
        mlb_events = {
            eid: ev for eid, ev in events.items()
            if str(ev.get("competitionId")) == FD_MLB_COMP_ID
        }
        log.info("Found %s FanDuel MLB events (filtered from %s total)", len(mlb_events), len(events))
        return mlb_events
    except Exception as exc:
        log.error("Failed to fetch FD MLB events: %s", exc)
        return {}


# ── Step 2: For each event, fetch pitcher K prop markets ─────────────────

def fetch_fd_k_markets(event_id: str, state: str = "nj"):
    """
    Fetch all pitcher strikeout O/U markets for a single FanDuel event.
    Returns list of {pitcher_name, line, side, odds, market_id, selection_id, ...}
    """
    base = _fd_api_base(state)

    # pitcher-props tab has all K markets
    markets_found = {}
    for tab in ("pitcher-props", "popular"):
        url = (
            f"{base}/event-page"
            f"?eventId={event_id}&tab={tab}&_ak={FD_API_KEY}"
        )
        try:
            data = _api_get(url)
            markets = data.get("attachments", {}).get("markets", {})
            for mid, market in markets.items():
                mtype = (market.get("marketType") or "").upper()

                # Standard O/U: type ends with TOTAL_STRIKEOUTS
                # e.g. PITCHER_C_TOTAL_STRIKEOUTS, PITCHER_E_TOTAL_STRIKEOUTS
                if "TOTAL_STRIKEOUTS" in mtype:
                    markets_found[mid] = (market, False)
                # Alt K markets: contains STRIKEOUTS but NOT TOTAL_STRIKEOUTS
                elif "STRIKEOUTS" in mtype and "TOTAL" not in mtype:
                    markets_found[mid] = (market, True)
        except Exception as exc:
            log.debug("Tab %s failed for event %s: %s", tab, event_id, exc)
            continue

        if markets_found:
            break
        time.sleep(0.3)

    if not markets_found:
        return []

    # Parse runners (over/under) from each market
    results = []
    for market_id, (market, is_alt) in markets_found.items():
        runners = market.get("runners", [])
        market_name = market.get("marketName", "")

        # Extract pitcher name: "Garrett Crochet - Strikeouts" → "Garrett Crochet"
        pitcher_name = ""
        for part in market_name.split(" - "):
            part = part.strip()
            if "strikeout" not in part.lower():
                pitcher_name = part
                break

        for runner in runners:
            # Runner name like "Garrett Crochet Over" or "Bailey Ober Under"
            runner_name_raw = (runner.get("runnerName") or "").strip()
            runner_lower = runner_name_raw.lower()

            if runner_lower.endswith(" over"):
                side = "over"
            elif runner_lower.endswith(" under"):
                side = "under"
            else:
                continue

            sel_id = runner.get("selectionId")
            handicap = runner.get("handicap")  # The K line (e.g., 7.5)
            odds_data = runner.get("winRunnerOdds", {})
            american = odds_data.get("americanDisplayOdds", {}).get("americanOddsInt")

            # Build FD deep link
            fd_link = (
                f"{FD_BASE_URL}/addToBetslip"
                f"?marketId[0]={market_id}&selectionId[0]={sel_id}"
            ) if sel_id else ""

            results.append({
                "event_id": event_id,
                "pitcher_name": pitcher_name,
                "line": sf(handicap),
                "side": side,
                "odds": si(american),
                "market_id": str(market_id),
                "selection_id": str(sel_id) if sel_id else "",
                "deep_link": fd_link,
                "market_name": market_name,
                "is_alternate": is_alt,
            })

    return results


# ── Step 3: Match FD events to game_pk and merge into BQ ────────────────

def fetch_today_games():
    """Get today's games from raw_game_weather to map team names → game_pk."""
    try:
        rows = list(bq_client.query(f"""
            SELECT game_pk, home_team_name, away_team_name
            FROM `{PROJECT}.{DATASET}.raw_game_weather`
            WHERE run_date = '{TODAY}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY ingested_at DESC) = 1
        """).result())
        return {r.game_pk: r for r in rows}
    except Exception:
        return {}


def match_event_to_game(event_name, games):
    """Match a FanDuel event name to a game_pk by team name."""
    ev_lower = (event_name or "").lower()
    for game_pk, g in games.items():
        home = (g.home_team_name or "").lower()
        away = (g.away_team_name or "").lower()
        if not home or not away:
            continue
        # Match by last word of team name (e.g. "Yankees", "Dodgers")
        home_last = home.split()[-1]
        away_last = away.split()[-1]
        if home_last in ev_lower and away_last in ev_lower:
            return game_pk
    return None


def match_pitcher_to_id(pitcher_name, game_pk):
    """Look up pitcher_id from raw_pitcher_matchup by name."""
    if not pitcher_name:
        return None
    try:
        rows = list(bq_client.query(f"""
            SELECT DISTINCT pitcher_id, pitcher_name
            FROM `{PROJECT}.{DATASET}.raw_pitcher_matchup`
            WHERE run_date = '{TODAY}' AND game_pk = {game_pk}
        """).result())
        # Fuzzy match by last name
        name_lower = pitcher_name.lower().strip()
        for r in rows:
            if r.pitcher_name and r.pitcher_name.lower().strip() == name_lower:
                return r.pitcher_id
            # Last name match
            if r.pitcher_name and r.pitcher_name.lower().split()[-1] == name_lower.split()[-1]:
                return r.pitcher_id
    except Exception:
        pass
    return None


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--state", default=FD_STATE)
    args = parser.parse_args()

    log.info("FanDuel K Prop Scraper — %s (state: %s)", TODAY, args.state)

    events = fetch_fd_mlb_events(args.state)
    if not events:
        log.warning("No FanDuel MLB events found")
        return

    games = fetch_today_games()
    log.info("Loaded %s games from BQ for matching", len(games))

    all_rows = []
    for eid, event in events.items():
        event_name = event.get("name", "")
        game_pk = match_event_to_game(event_name, games)

        k_markets = fetch_fd_k_markets(str(eid), args.state)
        if not k_markets:
            continue

        log.info("  Event %s (%s): %s K markets", eid, event_name, len(k_markets))

        for m in k_markets:
            pitcher_id = match_pitcher_to_id(m["pitcher_name"], game_pk) if game_pk else None

            row = {
                "run_date": TODAY.isoformat(),
                "game_pk": game_pk,
                "pitcher_id": pitcher_id,
                "pitcher_name": m["pitcher_name"],
                "team_code": "",
                "opp_team_code": "",
                "line": m["line"],
                "over_under": m["side"],
                "best_price": m["odds"],
                "best_book": "FanDuel",
                "pf_rating": None,
                "hit_rate_l10": "",
                "hit_rate_season": "",
                "hit_rate_vs_team": "",
                "avg_l10": None,
                "avg_home_away": None,
                "avg_vs_opponent": None,
                "streak": None,
                "deep_link_desktop": m["deep_link"],
                "deep_link_ios": "",
                "is_alternate": m.get("is_alternate", False),
                "fd_market_id": m["market_id"],
                "fd_selection_id": m["selection_id"],
                "ingested_at": NOW.isoformat(),
            }
            all_rows.append(row)

        time.sleep(0.5)  # Rate limit

    # Separate overs and unders for logging
    overs = [r for r in all_rows if r["over_under"] == "over"]
    unders = [r for r in all_rows if r["over_under"] == "under"]
    alts = [r for r in all_rows if r.get("is_alternate")]
    log.info("Scraped %s overs + %s unders (%s alt) = %s total K props",
             len(overs), len(unders), len(alts), len(all_rows))

    if args.dry_run:
        for r in all_rows:
            side_em = "O" if r["over_under"] == "over" else "U"
            print(
                f"  [{side_em}] {r['pitcher_name']:<22} {r['line']:.1f}  "
                f"{r['best_price']:+d}  gk={r['game_pk']}  pid={r['pitcher_id']}"
            )
        return

    if all_rows:
        table_ref = f"{PROJECT}.{DATASET}.raw_k_props"
        errors = bq_client.insert_rows_json(table_ref, all_rows)
        if errors:
            log.error("BQ insert errors: %s", errors[:3])
        else:
            log.info("Wrote %s FD K props to raw_k_props", len(all_rows))


if __name__ == "__main__":
    main()
