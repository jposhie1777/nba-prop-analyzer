# propfinder/hit_discord_alerts.py

"""
hit_discord_alerts.py — Send batter hit pick alerts to Discord #hit-bets channel.

Reads today's FIRE and STRONG hit picks from BigQuery and sends
rich embed messages with batter contact stats and pitcher vulnerability.
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
HIT_TABLE = f"{PROJECT}.{DATASET}.hit_picks_daily"

BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
HIT_CHANNEL_ID = os.getenv("DISCORD_HIT_CHANNEL_ID") or "1493361988338974882"
DISCORD_API = "https://discord.com/api/v10"

ET = ZoneInfo("America/New_York")
TODAY = datetime.now(ET).date()

GRADE_COLORS = {
    "FIRE":   0xEF4444,   # red
    "STRONG": 0xF59E0B,   # amber
    "LEAN":   0x6366F1,   # indigo
}


DK_EMOJI = {"name": "dk", "id": "1493069919766708295"}
FD_EMOJI = {"name": "fd", "id": "1493070566809403593"}


def _build_dk_link(outcome_code, event_id):
    if not outcome_code or not event_id:
        return None
    return f"https://sportsbook.draftkings.com/event/{event_id}?outcomes={outcome_code}"


def _build_fd_link(market_id, selection_id):
    if not market_id or not selection_id:
        return None
    return f"https://sportsbook.fanduel.com/addToBetslip?marketId[0]={market_id}&selectionId[0]={selection_id}"


def _dk_fd_buttons(pick):
    """Build interactive DK/FD button components."""
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


def _fmt_odds(val):
    if val is None:
        return "--"
    v = int(val)
    return f"+{v}" if v > 0 else str(v)


def _fmt_pct(val, decimals=0):
    if val is None:
        return "--"
    return f"{val:.{decimals}f}%"


def fetch_top_hit_picks():
    """Fetch today's FIRE, STRONG, and LEAN hit picks."""
    client = bigquery.Client(project=PROJECT)
    query = f"""
    SELECT *
    FROM `{HIT_TABLE}`
    WHERE run_date = @run_date
      AND grade IN ('FIRE', 'STRONG', 'LEAN')
      AND (game_date IS NULL OR game_date > CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY batter_id
      ORDER BY run_timestamp DESC
    ) = 1
    ORDER BY score DESC
    """
    params = [bigquery.ScalarQueryParameter("run_date", "DATE", TODAY.isoformat())]
    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return list(job.result())


def _pick_line(p):
    """Build a compact embed description for a hit pick."""
    batter = p.get("batter_name", "?")
    team = p.get("team_code", "")
    opp = p.get("opp_team_code", "")
    score = int(p.get("score") or 0)
    odds = _fmt_odds(p.get("best_price"))
    book = p.get("best_book", "")
    ws = "\U0001f3af " if p.get("ws_batting_order") is not None else ""

    avg = p.get("batting_avg_vs_hand")
    avg_str = f"{avg:.3f}" if avg else "--"
    contact = _fmt_pct(p.get("contact_rate"))
    l15 = p.get("l15_hit_rate")
    l15_str = f"{l15:.3f}" if l15 else "--"
    whip = p.get("p_whip")
    whip_str = f"{whip:.2f}" if whip else "--"
    k_rate = p.get("p_k_rate")
    k_str = _fmt_pct(k_rate) if k_rate else "--"

    bvp_ab = p.get("bvp_ab")
    bvp_hits = p.get("bvp_hits")
    bvp_str = f"{bvp_hits}/{bvp_ab}" if bvp_ab and bvp_ab > 0 else "--"

    streak = int(p.get("streak") or 0)
    streak_str = f" | Streak {streak}" if streak >= 3 else ""

    desc = (
        f"{ws}**{batter}** ({team} vs {opp}) \u2014 **OVER 0.5 Hits**\n"
        f"Hit-Pulse **{score}** \u2022 {odds} {book}{streak_str}\n"
        f"AVG {avg_str} | Contact {contact} | L15 {l15_str} | WHIP {whip_str} | P-K% {k_str} | BvP {bvp_str}"
    )
    return desc


def _pick_embed(p, color):
    """Build a single pick embed."""
    return {"description": _pick_line(p), "color": color}


def _pick_message(p, color):
    """Build full message payload: embed + DK/FD buttons."""
    payload = {"embeds": [_pick_embed(p, color)]}
    if BOT_TOKEN:
        payload["components"] = _dk_fd_buttons(p)
    return payload


def _purge_channel():
    """Delete all messages in the hit channel before sending fresh picks."""
    import time as _time

    if not BOT_TOKEN or not HIT_CHANNEL_ID:
        log.info("Skipping hit channel purge (need BOT_TOKEN + HIT_CHANNEL_ID)")
        return

    headers = {
        "Authorization": f"Bot {BOT_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "PulseSports/1.0",
    }

    total_deleted = 0
    while True:
        fetch_url = f"{DISCORD_API}/channels/{HIT_CHANNEL_ID}/messages?limit=100"
        req = Request(fetch_url, headers=headers, method="GET")
        try:
            with urlopen(req, timeout=10) as resp:
                messages = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            log.error("Failed to fetch hit channel messages: %s", exc)
            break

        if not messages:
            break

        msg_ids = [m["id"] for m in messages]

        if len(msg_ids) == 1:
            del_url = f"{DISCORD_API}/channels/{HIT_CHANNEL_ID}/messages/{msg_ids[0]}"
            req = Request(del_url, headers=headers, method="DELETE")
            try:
                with urlopen(req, timeout=10):
                    total_deleted += 1
            except Exception:
                break
        else:
            bulk_url = f"{DISCORD_API}/channels/{HIT_CHANNEL_ID}/messages/bulk-delete"
            data = json.dumps({"messages": msg_ids}).encode("utf-8")
            req = Request(bulk_url, data=data, headers=headers, method="POST")
            try:
                with urlopen(req, timeout=10):
                    total_deleted += len(msg_ids)
            except Exception as exc:
                if "429" in str(exc):
                    _time.sleep(2)
                    continue
                log.warning("Bulk delete failed: %s — trying individual", exc)
                for mid in msg_ids:
                    del_url = f"{DISCORD_API}/channels/{HIT_CHANNEL_ID}/messages/{mid}"
                    req = Request(del_url, headers=headers, method="DELETE")
                    try:
                        with urlopen(req, timeout=10):
                            total_deleted += 1
                    except Exception:
                        pass
                    _time.sleep(0.3)
                break

        _time.sleep(1)

    log.info("Purged %s messages from hit channel", total_deleted)


def _send_discord(payload, silent=True):
    """Send a message to Discord via bot token.

    silent=True adds the SUPPRESS_NOTIFICATIONS flag (1 << 12) so the message
    posts without triggering a push notification. Only the run-header message
    should pass silent=False — that way each run produces exactly one ping.
    """
    import time as _time

    if not BOT_TOKEN:
        log.warning("No Discord bot token configured for hit alerts")
        return

    if silent:
        payload = {**payload, "flags": (payload.get("flags", 0) | 4096)}

    url = f"{DISCORD_API}/channels/{HIT_CHANNEL_ID}/messages"
    headers = {
        "Authorization": f"Bot {BOT_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "PulseSports/1.0",
    }
    data = json.dumps(payload).encode("utf-8")

    for attempt in range(4):
        req = Request(url, data=data, headers=headers, method="POST")
        try:
            with urlopen(req, timeout=10) as resp:
                log.info("Discord hit alert sent: HTTP %s", resp.status)
                return
        except Exception as exc:
            if "429" in str(exc):
                wait = 2 ** attempt
                log.warning("Rate limited, retrying in %ss", wait)
                _time.sleep(wait)
            else:
                log.error("Discord hit alert failed: %s", exc)
                return
    log.error("Discord hit alert failed after retries")


def send_picks_to_discord(picks):
    """Send individual pick cards with DK/FD buttons."""
    import time as _time

    if not picks:
        return

    fire = [p for p in picks if p.get("grade") == "FIRE"]
    fire.sort(key=lambda p: (p.get("ws_batting_order") is not None, p.get("score") or 0), reverse=True)
    strong = [p for p in picks if p.get("grade") == "STRONG"]
    strong.sort(key=lambda p: (p.get("ws_batting_order") is not None, p.get("score") or 0), reverse=True)
    lean = [p for p in picks if p.get("grade") == "LEAN"]
    lean.sort(key=lambda p: (p.get("ws_batting_order") is not None, p.get("score") or 0), reverse=True)

    # Header
    _send_discord({
        "content": (
            f"# \u26be Hit Picks \u2014 {TODAY.strftime('%b %d, %Y')}\n"
            f"**{len(fire)}** FIRE + **{len(strong)}** STRONG + **{len(lean)}** LEAN matchups\n"
            f"\U0001f3af = Pitcher Weak Spot (vulnerable at batter's lineup position)"
        ),
    }, silent=False)
    _time.sleep(0.5)

    # FIRE picks — all
    if fire:
        _send_discord({"content": f"## \U0001f525 FIRE Hits ({len(fire)})"})
        _time.sleep(0.3)
        for p in fire:
            _send_discord(_pick_message(p, GRADE_COLORS["FIRE"]))
            _time.sleep(0.6)

    # STRONG picks — all
    if strong:
        _send_discord({"content": f"## \U0001f7e0 STRONG Hits ({len(strong)})"})
        _time.sleep(0.3)
        for p in strong:
            _send_discord(_pick_message(p, GRADE_COLORS["STRONG"]))
            _time.sleep(0.6)

    # LEAN picks — top 10
    lean_top = lean[:10]
    if lean_top:
        _send_discord({"content": f"## \U0001f535 LEAN Hits (top {len(lean_top)})"})
        _time.sleep(0.3)
        for p in lean_top:
            _send_discord(_pick_message(p, GRADE_COLORS["LEAN"]))
            _time.sleep(0.6)


def main():
    log.info("Fetching hit picks for %s", TODAY)
    picks = fetch_top_hit_picks()

    fire = sum(1 for p in picks if p.get("grade") == "FIRE")
    strong = sum(1 for p in picks if p.get("grade") == "STRONG")
    lean = sum(1 for p in picks if p.get("grade") == "LEAN")
    log.info("Found %s FIRE + %s STRONG + %s LEAN hit picks", fire, strong, lean)

    if not picks:
        log.info("No actionable hit picks today — skipping Discord alert")
        return

    _purge_channel()
    send_picks_to_discord(picks)
    log.info("Discord hit alerts sent to #hit-bets")


if __name__ == "__main__":
    main()
