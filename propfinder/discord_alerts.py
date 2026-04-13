"""
discord_alerts.py — Send HR pick alerts to Discord via bot token.

Reads today's IDEAL and FAVORABLE picks from BigQuery and sends
rich embed messages with interactive DK/FD buttons to #hr-bets.

Uses the bot token (not webhook) so interactive button components
work for parlay building via bot.py's PickView handler.
"""

import json
import logging
import os
from datetime import datetime
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from google.cloud import bigquery

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "graphite-flare-477419-h7")
DATASET = "propfinder"
HR_TABLE = f"{PROJECT}.{DATASET}.hr_picks_daily"

BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
CHANNEL_ID = os.getenv("DISCORD_HR_CHANNEL_ID", "1493058403990507622")
DISCORD_API = "https://discord.com/api/v10"

# Fallback webhook (used if bot token not available)
WEBHOOK_URL = os.getenv(
    "DISCORD_HR_WEBHOOK",
    "https://discord.com/api/webhooks/1493058679153623060/hEI1Eqxbdc2MyDFG-dAaAgoqMk46J11M9EKzM3ZJkSj-cZJZWvwSE1RM5u0MxlUMmZnD",
)

ET = ZoneInfo("America/New_York")
TODAY = datetime.now(ET).date()

GRADE_COLORS = {
    "IDEAL": 0x22C55E,       # green
    "FAVORABLE": 0xF59E0B,   # amber
}

GRADE_EMOJI = {
    "IDEAL": "\U0001f7e2",       # green circle
    "FAVORABLE": "\U0001f7e1",   # yellow circle
}


def _hand_label(side):
    s = (side or "R").upper()
    return "LHB" if s == "L" else "SHB" if s == "S" else "RHB"


def _pitcher_hand(side):
    s = (side or "R").upper()
    return "LHP" if s.startswith("L") else "RHP"


def _fmt_odds(val):
    if val is None:
        return "—"
    v = int(val)
    return f"+{v}" if v > 0 else str(v)


def _build_dk_link(outcome_code, event_id):
    if not outcome_code or not event_id:
        return None
    return f"https://sportsbook.draftkings.com/event/{event_id}?outcomes={outcome_code}"


def _build_fd_link(market_id, selection_id):
    if not market_id or not selection_id:
        return None
    return f"https://sportsbook.fanduel.com/addToBetslip?marketId[0]={market_id}&selectionId[0]={selection_id}"


def _best_book_line(pick):
    """Return (odds_str, book_label, deeplink) for DK or FD only."""
    price = pick.get("hr_odds_best_price")
    book = (pick.get("hr_odds_best_book") or "").strip()

    dk_link = _build_dk_link(pick.get("dk_outcome_code"), pick.get("dk_event_id"))
    fd_link = _build_fd_link(pick.get("fd_market_id"), pick.get("fd_selection_id"))

    # If best book IS DK or FD, use that price + link
    if "draftkings" in book.lower() or "dk" in book.lower():
        return _fmt_odds(price), "DK", dk_link
    if "fanduel" in book.lower() or "fd" in book.lower():
        return _fmt_odds(price), "FD", fd_link

    # Best book is neither — show best price, link to DK (preferred) or FD
    odds_str = _fmt_odds(price)
    if dk_link:
        return odds_str, "DK", dk_link
    if fd_link:
        return odds_str, "FD", fd_link
    return odds_str, "", None


def fetch_top_picks():
    """Fetch today's IDEAL and FAVORABLE picks from BigQuery."""
    client = bigquery.Client(project=PROJECT)
    query = f"""
    SELECT
      batter_name, bat_side, pitcher_name, pitcher_hand,
      home_team, away_team, batter_team, pitcher_team,
      score, grade, why, flags,
      iso, slg, l15_ev, l15_barrel_pct, season_ev, season_barrel_pct,
      l15_hard_hit_pct, hr_fb_pct,
      p_hr9_vs_hand, p_hr_fb_pct, p_barrel_pct, p_fb_pct,
      hr_odds_best_price, hr_odds_best_book,
      dk_outcome_code, dk_event_id, fd_market_id, fd_selection_id,
      weather_indicator, game_temp, wind_speed, wind_dir, precip_prob,
      conditions, ballpark_name, roof_type,
      home_moneyline, away_moneyline, over_under,
      bvp_ab, bvp_hits, bvp_hr,
      batting_order_pos, ws_batting_order, ws_at_bats, ws_hits, ws_home_runs, ws_slg,
      game_pk
    FROM `{HR_TABLE}`
    WHERE run_date = @run_date
      AND grade IN ('IDEAL', 'FAVORABLE')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY batter_name, pitcher_name
      ORDER BY run_timestamp DESC
    ) = 1
    ORDER BY score DESC
    """
    params = [bigquery.ScalarQueryParameter("run_date", "DATE", TODAY.isoformat())]
    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return list(job.result())


WEAK_SPOT_COLOR = 0xEF4444

COMPASS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
           "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]


def _wind_str(speed, direction):
    """Format wind as '12 mph SW'. Suppress if calm (0 mph)."""
    if not speed or int(speed) == 0:
        return ""
    spd = int(speed)
    if direction is not None:
        idx = round(int(direction) / 22.5) % 16
        return f"{spd} mph {COMPASS[idx]}"
    return f"{spd} mph"


def _weather_circle(indicator, precip_prob):
    """Green/Yellow/Red circle based on weather indicator or precip probability."""
    ind = (indicator or "").lower()
    if ind == "red" or (precip_prob and precip_prob >= 50):
        return "\U0001f534"   # red — high postponement risk
    if ind == "yellow" or (precip_prob and precip_prob >= 25):
        return "\U0001f7e1"   # yellow — some risk
    return "\U0001f7e2"       # green — clear


def _conditions_label(conditions, precip_prob):
    """Short forecast label."""
    c = (conditions or "").strip()
    if c:
        return c
    if precip_prob and precip_prob >= 50:
        return "Rain likely"
    if precip_prob and precip_prob >= 25:
        return "Chance of rain"
    return "Clear"


def _venue_weather_short(p):
    """Compact venue + weather: 'Sutter Health Park | 5 mph SW | Clear'."""
    parts = []
    park = p.get("ballpark_name") or ""
    roof = (p.get("roof_type") or "").lower()
    if park:
        parts.append(park)

    wind = _wind_str(p.get("wind_speed"), p.get("wind_dir"))
    if wind and "retractable" not in roof and "dome" not in roof:
        parts.append(wind)
    elif "dome" in roof or "retractable" in roof:
        parts.append("Dome")

    forecast = _conditions_label(p.get("conditions"), p.get("precip_prob"))
    circle = _weather_circle(p.get("weather_indicator"), p.get("precip_prob"))
    parts.append(f"{circle} {forecast}")

    return " | ".join(parts)


def _stats_line(p):
    """Compact stats: ISO | Barrel | EV | HH% | HR/FB% | BvP | P-HR/9."""
    parts = []
    iso = p.get("iso")
    if iso is not None:
        parts.append(f"ISO {iso:.3f}")
    barrel = p.get("l15_barrel_pct")
    if barrel is not None:
        parts.append(f"Barrel {barrel:.0f}%")
    ev = p.get("l15_ev")
    if ev is not None:
        parts.append(f"EV {ev:.1f}")
    hh = p.get("l15_hard_hit_pct")
    if hh is not None:
        parts.append(f"HH {hh:.0f}%")
    hrfb = p.get("hr_fb_pct")
    if hrfb is not None:
        parts.append(f"HR/FB {hrfb:.0f}%")

    bvp_ab = p.get("bvp_ab")
    bvp_hits = p.get("bvp_hits")
    bvp_hr = p.get("bvp_hr")
    if bvp_ab and bvp_ab > 0:
        bvp_str = f"BvP {bvp_hits}-{bvp_ab}"
        if bvp_hr and bvp_hr > 0:
            bvp_str += f", {bvp_hr} HR"
        parts.append(bvp_str)

    phr9 = p.get("p_hr9_vs_hand")
    if phr9 is not None:
        parts.append(f"P-HR/9 {phr9:.2f}")
    return " | ".join(parts)


DK_EMOJI = {"name": "dk", "id": "1493069919766708295"}
FD_EMOJI = {"name": "fd", "id": "1493070566809403593"}


def _dk_fd_buttons(pick):
    """Build interactive DK/FD button components (requires bot token, not webhook)."""
    pick_id = f"{pick.get('batter_name', '?')}_{pick.get('game_pk', '')}"
    buttons = []
    buttons.append({
        "type": 2, "style": 2,
        "label": "DK",
        "custom_id": f"dk:{pick_id}",
        "emoji": DK_EMOJI,
    })
    buttons.append({
        "type": 2, "style": 2,
        "label": "FD",
        "custom_id": f"fd:{pick_id}",
        "emoji": FD_EMOJI,
    })
    return [{"type": 1, "components": buttons}]


def _dk_fd_links(pick):
    """Fallback markdown links when bot token not available."""
    dk_link = _build_dk_link(pick.get("dk_outcome_code"), pick.get("dk_event_id"))
    fd_link = _build_fd_link(pick.get("fd_market_id"), pick.get("fd_selection_id"))
    parts = []
    if dk_link:
        parts.append(f"[<:dk:1493069919766708295> DK]({dk_link})")
    if fd_link:
        parts.append(f"[<:fd:1493070566809403593> FD]({fd_link})")
    return " \u2022 ".join(parts)


def _send_discord(payload):
    """Send a message to Discord. Uses bot token if available, webhook as fallback."""
    import time as _time

    if BOT_TOKEN:
        url = f"{DISCORD_API}/channels/{CHANNEL_ID}/messages"
        headers = {
            "Authorization": f"Bot {BOT_TOKEN}",
            "Content-Type": "application/json",
            "User-Agent": "PulseSports/1.0",
        }
    elif WEBHOOK_URL:
        url = WEBHOOK_URL
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "PulseSports/1.0",
        }
    else:
        log.warning("No Discord bot token or webhook URL configured")
        return

    data = json.dumps(payload).encode("utf-8")

    for attempt in range(4):
        req = Request(url, data=data, headers=headers, method="POST")
        try:
            with urlopen(req, timeout=10) as resp:
                log.info("Discord sent: HTTP %s", resp.status)
                return
        except Exception as exc:
            err_str = str(exc)
            if "429" in err_str:
                wait = 2 ** attempt
                log.warning("Rate limited, retrying in %ss (attempt %s)", wait, attempt + 1)
                _time.sleep(wait)
            else:
                log.error("Discord send failed: %s", exc)
                return
    log.error("Discord send failed after retries")


def _is_weak_spot(p):
    """True weak spot: pitcher is vulnerable at this batter's specific lineup position."""
    return p.get("ws_batting_order") is not None


def _pick_embed(p, color):
    """Build a compact embed. Prefixes with target emoji if weak spot."""
    batter = p.get("batter_name", "?")
    bat = _hand_label(p.get("bat_side"))
    pitcher = p.get("pitcher_name", "?")
    phand = _pitcher_hand(p.get("pitcher_hand"))
    bt = p.get("batter_team") or ""
    pt = p.get("pitcher_team") or ""
    score = int(p.get("score") or 0)
    odds = _fmt_odds(p.get("hr_odds_best_price"))
    ws = "\U0001f3af " if _is_weak_spot(p) else ""

    desc = (
        f"{ws}**{batter}** ({bat}) {bt} vs {pitcher} ({phand}) {pt}\n"
        f"Pulse **{score}** \u2022 {odds} | {_venue_weather_short(p)}\n"
        f"{_stats_line(p)}"
    )

    # If no bot token, append markdown links as fallback
    if not BOT_TOKEN:
        links = _dk_fd_links(p)
        if links:
            desc += f"\n{links}"

    return {"description": desc, "color": color}


def _pick_message(p, color):
    """Build the full message payload for a pick (embed + buttons)."""
    payload = {"embeds": [_pick_embed(p, color)]}
    if BOT_TOKEN:
        payload["components"] = _dk_fd_buttons(p)
    return payload


def send_picks_to_discord(picks):
    """Send individual pick cards with DK/FD link buttons."""
    import time as _time

    if not picks:
        return

    ideal_count = sum(1 for p in picks if p.get("grade") == "IDEAL")
    fav_count = sum(1 for p in picks if p.get("grade") == "FAVORABLE")

    # Header message
    _send_discord({
        "content": (
            f"# \u26be HR Picks \u2014 {TODAY.strftime('%b %d, %Y')}\n"
            f"**{ideal_count}** IDEAL + **{fav_count}** FAVORABLE matchups\n"
            f"\U0001f3af = Pitcher Weak Spot (vulnerable at batter's lineup position)"
        ),
    })
    _time.sleep(0.5)

    # ALL IDEAL picks
    ideal_picks = [p for p in picks if p.get("grade") == "IDEAL"]
    if ideal_picks:
        _send_discord({"content": f"## \U0001f7e2 IDEAL Matchups ({len(ideal_picks)})"})
        _time.sleep(0.3)
        for p in ideal_picks:
            _send_discord(_pick_message(p, GRADE_COLORS["IDEAL"]))
            _time.sleep(0.6)

    # Top 10 FAVORABLE — weak spots sorted first, then by score
    fav_picks = [p for p in picks if p.get("grade") == "FAVORABLE"]
    fav_picks.sort(key=lambda p: (_is_weak_spot(p), p.get("score") or 0), reverse=True)
    fav_top = fav_picks[:10]

    if fav_top:
        _send_discord({"content": f"## \U0001f7e1 FAVORABLE Matchups (top {len(fav_top)})"})
        _time.sleep(0.3)
        for p in fav_top:
            _send_discord(_pick_message(p, GRADE_COLORS["FAVORABLE"]))
            _time.sleep(0.6)


def main():
    log.info("Fetching HR picks for %s", TODAY)
    picks = fetch_top_picks()

    ideal = sum(1 for p in picks if p.get("grade") == "IDEAL")
    fav = sum(1 for p in picks if p.get("grade") == "FAVORABLE")
    log.info("Found %s IDEAL + %s FAVORABLE picks", ideal, fav)

    if not picks:
        log.info("No IDEAL/FAVORABLE picks today — skipping Discord alert")
        return

    send_picks_to_discord(picks)
    log.info("Discord HR alerts sent (%s picks)", len(picks))


if __name__ == "__main__":
    main()
